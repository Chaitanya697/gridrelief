"""allocation-service: consumes request.created, ranks requests, recommends suppliers."""
import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, Optional

import httpx
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
CENTER_URL = os.getenv("CENTER_SERVICE_URL", "http://localhost:8001")
EXCHANGE = "gridrelief.events"
QUEUE = "allocation.request-created"  # durable: messages wait here while the service is down
URGENCY_WEIGHT = {"critical": 3, "high": 2, "normal": 1}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [allocation] %(message)s")
logging.getLogger("pika").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("allocation")

lock = threading.RLock()
seen_events = set()    # event_ids already processed (idempotent consumer)
requests_db = {}       # request_id -> request
recommendations = {}   # request_id -> recommendation


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def publish(event_type: str, actor: str, data: dict) -> None:
    """Best-effort publish with a short timeout so it can never stall the allocation lock."""
    event = {"event_id": str(uuid.uuid4()), "type": event_type, "timestamp": now_iso(),
             "actor": actor, "data": data}
    try:
        params = pika.URLParameters(RABBIT_URL)
        params.socket_timeout = 2
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
        ch.basic_publish(EXCHANGE, event_type, json.dumps(event),
                         pika.BasicProperties(content_type="application/json", delivery_mode=2))
        conn.close()
    except Exception as exc:
        log.warning("could not publish %s: %s", event_type, exc)


def score(r: dict) -> float:
    """urgency weight x 10 + hours waiting x 0.5 + affected people / 100 (configurable rule)."""
    created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
    hours = max(0.0, (datetime.now(timezone.utc) - created).total_seconds() / 3600)
    return round(URGENCY_WEIGHT[r["urgency"]] * 10 + hours * 0.5 + r["affected_people"] / 100, 2)


def fetch_stock(item: str):
    try:
        resp = httpx.get(f"{CENTER_URL}/inventory", params={"item": item}, timeout=5)
        resp.raise_for_status()
        return {c["center_id"]: c["shareable_quantity"] for c in resp.json()}
    except Exception as exc:
        log.warning("could not fetch stock for %s: %s", item, exc)
        return None


def recompute() -> None:
    """Rank all pending requests per item and split the shareable stock in priority order."""
    changed = []
    with lock:
        for item in {r["item"] for r in requests_db.values()}:
            stock = fetch_stock(item)
            if stock is None:
                continue
            for rec in recommendations.values():  # stock already promised by supplier approvals
                if rec["item"] == item:
                    for a in rec.get("approved_allocations", []):
                        stock[a["supplier"]] = stock.get(a["supplier"], 0) - a["quantity"]
            pending = [r for r in requests_db.values() if r["item"] == item and r["status"] == "pending"]
            for r in sorted(pending, key=score, reverse=True):
                remaining, allocations = r["quantity"], []
                for supplier in list(stock):
                    take = min(stock[supplier], remaining)
                    if supplier == r["center_id"] or take <= 0:
                        continue
                    allocations.append({"supplier": supplier, "quantity": take})
                    stock[supplier] -= take
                    remaining -= take
                rec = {"request_id": r["request_id"], "requester": r["center_id"], "item": item,
                       "urgency": r["urgency"], "requested_quantity": r["quantity"],
                       "recommended_quantity": r["quantity"] - remaining,
                       "allocations": allocations, "score": score(r), "status": "pending"}
                old = recommendations.get(r["request_id"])
                recommendations[r["request_id"]] = rec
                if old is None or old["allocations"] != allocations:  # log/publish only real changes
                    log.info("recommendation %s: %s gets %s of %s (score %s) from %s", r["request_id"],
                             r["center_id"], rec["recommended_quantity"], r["quantity"], rec["score"], allocations)
                    changed.append(rec)
    for rec in changed:
        publish("allocation.recommended", "allocation-engine", {
            k: rec[k] for k in ("request_id", "requester", "item", "requested_quantity",
                                "recommended_quantity", "allocations", "score")})


def handle(ch, method, props, body):
    try:
        event = json.loads(body)
        new_request = False
        with lock:
            if event["event_id"] in seen_events:
                log.info("duplicate event %s ignored", event["event_id"])
            else:
                seen_events.add(event["event_id"])
                d = event["data"]
                requests_db[d["request_id"]] = {
                    "request_id": d["request_id"], "center_id": d["center_id"], "item": d["item"],
                    "quantity": d["quantity"], "urgency": d["urgency"],
                    "affected_people": d.get("affected_people", 0),
                    "created_at": event["timestamp"], "status": "pending"}
                log.info("handled %s %s (%s x %s, %s)", event["type"], d["request_id"],
                         d["item"], d["quantity"], d["urgency"])
                new_request = True
        if new_request:
            recompute()
    except Exception as exc:
        log.exception("failed to handle message: %s", exc)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_forever():
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            ch = conn.channel()
            ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
            ch.queue_declare(QUEUE, durable=True)
            ch.queue_bind(QUEUE, EXCHANGE, "request.created")
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(QUEUE, handle)
            log.info("consuming from queue %s", QUEUE)
            ch.start_consuming()
        except Exception as exc:
            log.warning("consumer error, retrying in 3s: %s", exc)
            time.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=consume_forever, daemon=True).start()
    yield


app = FastAPI(title="GridRelief allocation-service", lifespan=lifespan)


class Decision(BaseModel):
    decision: Literal["approve", "partial", "reject"]
    quantity: Optional[int] = None
    reason: Optional[str] = None
    operator: str = "supplier-operator-1"


@app.get("/recommendations")
def list_recommendations():
    recompute()
    return sorted(recommendations.values(), key=lambda r: r["score"], reverse=True)


@app.get("/recommendations/{request_id}")
def get_recommendation(request_id: str):
    if request_id not in recommendations:
        raise HTTPException(404, "no recommendation for this request")
    return recommendations[request_id]


@app.post("/recommendations/{request_id}/decision")
def decide(request_id: str, body: Decision):
    with lock:
        rec = recommendations.get(request_id)
        if rec is None:
            raise HTTPException(404, "no recommendation for this request")
        if rec["status"] != "pending":
            raise HTTPException(409, f"already {rec['status']}")
        if body.decision == "reject":
            if not body.reason:
                raise HTTPException(400, "a rejection needs a reason")
            rec["status"], rec["approved_allocations"] = "rejected", []
            event = ("request.rejected", {"request_id": request_id, "requester": rec["requester"],
                                          "reason": body.reason})
        else:
            total = rec["recommended_quantity"]
            qty = total if body.decision == "approve" else body.quantity
            if qty is None or qty <= 0 or qty > total:
                raise HTTPException(400, f"quantity must be between 1 and {total}")
            approved, left = [], qty
            for a in rec["allocations"]:
                take = min(a["quantity"], left)
                if take > 0:
                    approved.append({"supplier": a["supplier"], "quantity": take})
                    left -= take
            rec["status"] = "approved" if qty == total else "partial"
            rec["approved_allocations"] = approved
            event = ("request.approved", {"request_id": request_id, "requester": rec["requester"],
                                          "approved_quantity": qty, "status": rec["status"],
                                          "allocations": approved})
        rec["decision"] = {"decision": body.decision, "quantity": body.quantity, "reason": body.reason,
                           "operator": body.operator, "timestamp": now_iso()}
        requests_db[request_id]["status"] = rec["status"]
        log.info("decision on %s: %s by %s", request_id, rec["status"], body.operator)
    publish(event[0], body.operator, event[1])
    # TODO: reserve stock at the supplier (center-service), require two operators above a threshold,
    # write every event to audit-service.
    return rec
