import os
import json
import logging
import sys
from datetime import datetime, timezone

import asyncio
import asyncpg
import nats
from nats.js.api import ConsumerConfig
from fastapi import FastAPI, HTTPException

DB_DSN = os.environ.get("DB_DSN", "postgresql://audit:audit@audit-db:5432/audit")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("audit-svc")

app = FastAPI(title="audit-svc")
pool = None
nc = None
ready = {"db": False, "nats": False}

SUBJECTS = ["gitea.push", "gitea.issue.created", "gitea.issue.closed", "gitea.pull_request.merged"]


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "audit-svc")
    log.info(json.dumps(kwargs))


async def handle_event(msg):
    try:
        envelope = json.loads(msg.data.decode())
        async with pool.acquire() as conn:
            inserted = await conn.fetchval(
                """
                INSERT INTO audit_log (event_id, entity, actor, repo, occurred_at, request_id, raw)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                envelope["event_id"],
                envelope["type"],
                envelope["payload"].get("actor") or envelope["payload"].get("pusher", "desconocido"),
                envelope["payload"].get("repo"),
                datetime.fromisoformat(envelope["occurred_at"]),
                envelope.get("request_id"),
                json.dumps(envelope),
            )
        if inserted:
            jlog(level="info", message="registro guardado", event_id=envelope["event_id"])
        else:
            jlog(level="info", message="evento duplicado, ignorado (idempotencia)", event_id=envelope["event_id"])
        await msg.ack()
    except Exception as e:
        jlog(level="error", message=f"fallo al procesar evento: {e}")


@app.on_event("startup")
async def startup():
    global pool, nc
    pool = await asyncpg.create_pool(DB_DSN)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                event_id TEXT PRIMARY KEY,
                entity TEXT NOT NULL,
                actor TEXT,
                repo TEXT,
                occurred_at TIMESTAMPTZ,
                request_id TEXT,
                raw JSONB
            )
            """
        )
    ready["db"] = True

    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    # event-bridge crea el stream EVENTS al arrancar; si todavia no existe, reintentamos
    # en vez de asumir un orden de arranque exacto entre contenedores
    for attempt in range(10):
        try:
            for subject in SUBJECTS:
                await js.subscribe(
                    subject, durable=f"audit-svc-{subject.replace('.', '-')}", cb=handle_event, manual_ack=True,
                    config=ConsumerConfig(max_deliver=3, ack_wait=10),
                )
            break
        except Exception as e:
            jlog(level="warning", message=f"stream EVENTS aun no listo, reintento {attempt + 1}/10: {e}")
            await asyncio.sleep(2)
    else:
        jlog(level="error", message="no se pudo suscribir a los eventos tras 10 intentos")
    ready["nats"] = True
    jlog(level="info", message="suscripto a eventos de auditoria")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    if not ready["db"] or not ready["nats"]:
        raise HTTPException(status_code=503, detail="dependencias no disponibles")
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT count(*) FROM audit_log")
    return f"audit_entries_total {total}\n"


@app.get("/v2/audit")
async def get_audit(entity: str = None, id: str = None, from_: str = None, to: str = None):
    query = "SELECT event_id, entity, actor, repo, occurred_at, request_id FROM audit_log WHERE 1=1"
    params = []
    if entity:
        params.append(entity)
        query += f" AND entity = ${len(params)}"
    if id:
        params.append(f"%{id}%")
        query += f" AND repo LIKE ${len(params)}"
    query += " ORDER BY occurred_at DESC LIMIT 100"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    items = [
        {
            "event_id": str(r["event_id"]),
            "entity": r["entity"],
            "actor": r["actor"],
            "repo": r["repo"],
            "occurred_at": r["occurred_at"].isoformat(),
            "request_id": r["request_id"],
        }
        for r in rows
    ]
    return {"total": len(items), "items": items}
