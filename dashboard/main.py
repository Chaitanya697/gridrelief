"""dashboard: one web console for the whole GridRelief network (UI + a small API gateway)."""
import asyncio
import collections
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pika
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
CENTER = os.getenv("CENTER_URL", "http://center-service:8001")
REQUEST = os.getenv("REQUEST_URL", "http://request-service:8002")
ALLOC = os.getenv("ALLOCATION_URL", "http://allocation-service:8003")
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
EXCHANGE, QUEUE = "gridrelief.events", "allocation.request-created"
SERVICES = {"center": "center-service", "request": "request-service", "allocation": "allocation-service"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [dashboard] %(message)s")
logging.getLogger("pika").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("dashboard")

events = collections.deque(maxlen=60)                       # live event stream (newest first)
broker = {"ok_at": 0.0, "ready": None, "consumers": None}   # real-time queue depth
cache = {}                                                  # last known data per service


def listen_events():
    """Spy on every event flowing through the exchange (own exclusive queue, bound to '#')."""
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            ch = conn.channel()
            ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
            q = ch.queue_declare("", exclusive=True).method.queue
            ch.queue_bind(q, EXCHANGE, "#")

            def on_message(_ch, _method, _props, body):
                try:
                    events.appendleft(json.loads(body))
                except ValueError:
                    pass

            ch.basic_consume(q, on_message, auto_ack=True)
            log.info("listening to all events on %s", EXCHANGE)
            ch.start_consuming()
        except Exception as exc:
            log.warning("event listener retrying: %s", exc)
            time.sleep(3)


def monitor_queue():
    """Ask the broker (passive declare) how many messages wait and how many consumers are attached."""
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            while True:
                try:
                    ch = conn.channel()
                    m = ch.queue_declare(QUEUE, passive=True).method
                    broker.update(ok_at=time.time(), ready=m.message_count, consumers=m.consumer_count)
                    ch.close()
                except pika.exceptions.ChannelClosedByBroker:  # queue not created yet
                    broker.update(ok_at=time.time(), ready=None, consumers=None)
                conn.sleep(1)
        except Exception as exc:
            broker["ok_at"] = 0.0
            log.warning("queue monitor retrying: %s", exc)
            time.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for fn in (listen_events, monitor_queue):
        threading.Thread(target=fn, daemon=True).start()
    yield


app = FastAPI(title="GridRelief dashboard", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html", media_type="text/html; charset=utf-8")


async def fetch(client, key, url):
    try:
        r = await client.get(url, timeout=1.5)
        r.raise_for_status()
        cache[key] = r.json()
        return True
    except Exception:
        return False


@app.get("/api/state")
async def state():
    async with httpx.AsyncClient() as c:
        up = await asyncio.gather(fetch(c, "centers", f"{CENTER}/centers"),
                                  fetch(c, "requests", f"{REQUEST}/requests"),
                                  fetch(c, "recs", f"{ALLOC}/recommendations"))
    rabbit_up = time.time() - broker["ok_at"] < 5
    return {
        "services": {"rabbitmq": rabbit_up, "center": up[0], "request": up[1], "allocation": up[2]},
        "queue": {"ready": broker["ready"], "consumers": broker["consumers"]} if rabbit_up else None,
        "centers": cache.get("centers", []), "requests": cache.get("requests", []),
        "recommendations": cache.get("recs", []), "events": list(events)[:40],
        "can_control": os.path.exists(DOCKER_SOCK),
    }


async def forward(method, url, body=None):
    try:
        async with httpx.AsyncClient() as c:
            r = await c.request(method, url, json=body, timeout=5)
        return JSONResponse(r.json(), status_code=r.status_code)
    except (httpx.HTTPError, ValueError):
        return JSONResponse({"detail": "that service is unreachable (is it stopped?)"}, status_code=503)


@app.post("/api/requests")
async def new_request(req: Request):
    return await forward("POST", f"{REQUEST}/requests", await req.json())


@app.post("/api/decision/{request_id}")
async def decision(request_id: str, req: Request):
    return await forward("POST", f"{ALLOC}/recommendations/{request_id}/decision", await req.json())


@app.post("/api/centers")
async def new_center(req: Request):
    return await forward("POST", f"{CENTER}/centers", await req.json())


@app.put("/api/centers/{center_id}/inventory")
async def set_inventory(center_id: str, req: Request):
    return await forward("PUT", f"{CENTER}/centers/{center_id}/inventory", await req.json())


# ---- optional container controls through the Docker Engine API (needs /var/run/docker.sock) ----
def docker(method, path, **kw):
    with httpx.Client(transport=httpx.HTTPTransport(uds=DOCKER_SOCK), base_url="http://docker", timeout=30) as c:
        return c.request(method, path, **kw)


def container_of(service):
    flt = json.dumps({"label": [f"com.docker.compose.service={service}"]})
    items = docker("GET", "/containers/json", params={"all": "true", "filters": flt}).json()
    return items[0]["Id"] if items else None


def act(service, action):
    cid = container_of(service)
    if cid is None:
        raise HTTPException(404, f"container for {service} not found")
    r = docker("POST", f"/containers/{cid}/{action}", params={"t": "2"})
    if r.status_code not in (204, 304):
        raise HTTPException(500, f"docker said {r.status_code}")


@app.post("/api/service/{name}/{action}")
def control(name: str, action: str):
    if name not in SERVICES or action not in ("start", "stop", "restart"):
        raise HTTPException(404, "unknown service or action")
    if not os.path.exists(DOCKER_SOCK):
        raise HTTPException(501, "container controls need the docker.sock volume; use docker compose instead")
    act(SERVICES[name], action)
    log.info("%s %s", action, SERVICES[name])
    return {"ok": True}


@app.post("/api/reset")
def reset():
    """Fresh demo: purge the queue and restart the three services (their data is in memory)."""
    if not os.path.exists(DOCKER_SOCK):
        raise HTTPException(501, "reset needs the docker.sock volume; use docker compose down/up instead")
    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
        conn.channel().queue_purge(QUEUE)
        conn.close()
    except Exception:
        pass  # queue may not exist yet
    for service in ("allocation-service", "request-service", "center-service"):
        act(service, "restart")
    events.clear()
    cache.clear()
    log.info("demo reset")
    return {"ok": True}
