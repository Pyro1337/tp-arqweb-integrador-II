import os
import json
import uuid
import logging
import sys
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
import nats

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "changeme")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("event-bridge")

app = FastAPI(title="event-bridge")
nc = None
dlq_events = []


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "event-bridge")
    log.info(json.dumps(kwargs))


async def on_max_deliveries(msg):
    # advisory que NATS emite cuando un consumidor supera max_deliver: lo tratamos como DLQ
    try:
        advisory = json.loads(msg.data.decode())
    except Exception:
        advisory = {"raw": msg.data.decode(errors="ignore")}
    entry = {"received_at": datetime.now(timezone.utc).isoformat(), "advisory": advisory}
    dlq_events.append(entry)
    jlog(level="warning", message="mensaje movido a DLQ (max_deliver superado)", advisory=advisory)


@app.on_event("startup")
async def startup():
    global nc
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        await js.add_stream(name="EVENTS", subjects=["gitea.*", "export.*", "media.*"])
    except Exception as e:
        jlog(level="info", message=f"stream ya existia o no se pudo crear: {e}")
    # advisories de JetStream cuando un consumidor agota los reintentos (nuestra DLQ)
    await nc.subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>", cb=on_max_deliveries)
    jlog(level="info", message="conectado a NATS, stream EVENTS listo, escuchando DLQ")


@app.get("/dlq")
async def get_dlq():
    return {"total": len(dlq_events), "items": dlq_events}


def build_envelope(event_type: str, payload: dict, request_id: str, delivery_id: str) -> dict:
    # usamos el ID de entrega que manda Gitea (X-Gitea-Delivery) como event_id.
    # si Gitea reintenta la MISMA entrega (por un timeout, por ejemplo), este id se repite,
    # y asi los consumidores (audit-svc, search-svc) pueden detectar el duplicado.
    # si por algun motivo no viene ese header, generamos uno propio como respaldo.
    event_id = delivery_id or str(uuid.uuid4())
    return {
        "event_id": event_id,
        "type": event_type,
        "version": "v1",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "request_id": request_id,
        "payload": payload,
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    if nc is None or not nc.is_connected:
        raise HTTPException(status_code=503, detail="nats no disponible")
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    return "events_received_total 0\n"


@app.post("/webhook/{secret}")
async def webhook(secret: str, request: Request):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    delivery_id = request.headers.get("X-Gitea-Delivery", "")
    if secret != WEBHOOK_SECRET:
        jlog(level="warning", message="webhook rechazado, secreto invalido", request_id=request_id)
        raise HTTPException(status_code=401, detail="secreto invalido")

    body = await request.json()
    gitea_event = request.headers.get("X-Gitea-Event", "unknown")

    if gitea_event == "push":
        subject = "gitea.push"
        payload = {
            "repo": body.get("repository", {}).get("full_name"),
            "ref": body.get("ref"),
            "pusher": body.get("pusher", {}).get("login"),
            "commits": len(body.get("commits", [])),
        }
    elif gitea_event == "issues":
        action = body.get("action")
        subject = "gitea.issue.created" if action == "opened" else "gitea.issue.closed"
        payload = {
            "repo": body.get("repository", {}).get("full_name"),
            "issue_id": body.get("issue", {}).get("number"),
            "title": body.get("issue", {}).get("title"),
            "actor": body.get("sender", {}).get("login"),
        }
    else:
        jlog(level="info", message=f"evento ignorado: {gitea_event}", request_id=request_id)
        return {"status": "ignored", "event": gitea_event}

    envelope = build_envelope(subject, payload, request_id, delivery_id)
    await nc.publish(subject, json.dumps(envelope).encode())
    jlog(level="info", message=f"publicado {subject}", event_id=envelope["event_id"], request_id=request_id)
    return {"status": "published", "event_id": envelope["event_id"]}
