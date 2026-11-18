"""center-service: relief centers and their inventory (in-memory, seeded at startup)."""
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE = "gridrelief.events"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [center] %(message)s")
logging.getLogger("pika").setLevel(logging.CRITICAL)
log = logging.getLogger("center")

app = FastAPI(title="GridRelief center-service")

# Centers are data, not code: any number can be added through POST /centers.
centers = {
    "center-a": {"center_id": "center-a", "name": "Center A", "inventory": {
        "food_packets": {"quantity": 500, "shareable_quantity": 500}}},
    "center-b": {"center_id": "center-b", "name": "Center B", "inventory": {
        "food_packets": {"quantity": 0, "shareable_quantity": 0}}},
    "center-c": {"center_id": "center-c", "name": "Center C", "inventory": {
        "food_packets": {"quantity": 0, "shareable_quantity": 0}}},
}


class CenterIn(BaseModel):
    name: str
    center_id: Optional[str] = None


class InventoryIn(BaseModel):
    item: str
    quantity: int = Field(ge=0)
    shareable_quantity: int = Field(ge=0)


def publish(event_type: str, actor: str, data: dict) -> None:
    event = {"event_id": str(uuid.uuid4()), "type": event_type,
             "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "actor": actor, "data": data}
    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
        ch = conn.channel()
        ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
        ch.basic_publish(EXCHANGE, event_type, json.dumps(event),
                         pika.BasicProperties(content_type="application/json", delivery_mode=2))
        conn.close()
        log.info("published %s %s", event_type, event["event_id"])
    except Exception as exc:  # best effort for the prototype
        log.warning("could not publish %s: %s", event_type, exc)


@app.get("/centers")
def list_centers():
    return list(centers.values())


@app.post("/centers", status_code=201)
def register_center(body: CenterIn):
    cid = body.center_id or f"center-{uuid.uuid4().hex[:6]}"
    if cid in centers:
        raise HTTPException(409, "center already exists")
    centers[cid] = {"center_id": cid, "name": body.name, "inventory": {}}
    log.info("registered center %s", cid)
    # TODO: verified registration (operators, verification flag)
    return centers[cid]


@app.put("/centers/{center_id}/inventory")
def set_inventory(center_id: str, body: InventoryIn):
    if center_id not in centers:
        raise HTTPException(404, "unknown center")
    if body.shareable_quantity > body.quantity:
        raise HTTPException(400, "shareable_quantity cannot exceed quantity")
    centers[center_id]["inventory"][body.item] = {
        "quantity": body.quantity, "shareable_quantity": body.shareable_quantity}
    log.info("inventory set %s %s=%s shareable=%s", center_id, body.item,
             body.quantity, body.shareable_quantity)
    publish("inventory.updated", f"{center_id}-operator",
            {"center_id": center_id, "item": body.item, **centers[center_id]["inventory"][body.item]})
    return centers[center_id]


@app.get("/inventory")
def discover(item: str = "food_packets"):
    """Discovery: which centers have shareable stock of this item."""
    return [{"center_id": c["center_id"], "name": c["name"], "item": item,
             "shareable_quantity": c["inventory"][item]["shareable_quantity"]}
            for c in centers.values()
            if item in c["inventory"] and c["inventory"][item]["shareable_quantity"] > 0]
