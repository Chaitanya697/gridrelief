"""request-service: accepts resource requests and publishes request.created."""
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Literal

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE = "gridrelief.events"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [request] %(message)s")
logging.getLogger("pika").setLevel(logging.CRITICAL)
log = logging.getLogger("request")

app = FastAPI(title="GridRelief request-service")
requests_db = {}
lock = threading.Lock()


class RequestIn(BaseModel):
    center_id: str
    item: str = "food_packets"
    quantity: int = Field(gt=0)
    urgency: Literal["critical", "high", "normal"]
    affected_people: int = 0


def publish(event_type: str, actor: str, data: dict) -> dict:
    event = {"event_id": str(uuid.uuid4()), "type": event_type,
             "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "actor": actor, "data": data}
    for attempt in range(1, 6):  # producers retry if the broker is briefly unavailable
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            ch = conn.channel()
            ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
            ch.basic_publish(EXCHANGE, event_type, json.dumps(event),
                             pika.BasicProperties(content_type="application/json", delivery_mode=2))
            conn.close()
            log.info("published %s %s", event_type, event["event_id"])
            return event
        except Exception as exc:
            log.warning("publish attempt %d failed: %s", attempt, exc)
            time.sleep(1)
    raise HTTPException(503, "message broker unavailable")


@app.post("/requests", status_code=201)
def create_request(body: RequestIn):
    # TODO: check that center_id exists by calling center-service
    with lock:
        request_id = f"r-{len(requests_db) + 101}"
        record = {"request_id": request_id, **body.model_dump(), "status": "submitted"}
        requests_db[request_id] = record
    event = publish("request.created", f"{body.center_id}-operator", record)
    return {**record, "event_id": event["event_id"]}


@app.get("/requests")
def list_requests():
    return list(requests_db.values())
