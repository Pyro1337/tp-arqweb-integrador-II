import os
import json
import logging
import sys
import time
import re
from datetime import datetime, timezone

import asyncio
import asyncpg
import nats
from nats.js.api import ConsumerConfig
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, CONTENT_TYPE_LATEST, generate_latest

from opentelemetry import trace, propagate
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

JAEGER_OTLP_ENDPOINT = os.environ.get("JAEGER_OTLP_ENDPOINT", "")
trace.set_tracer_provider(TracerProvider(resource=Resource.create({"service.name": "audit-svc"})))
if JAEGER_OTLP_ENDPOINT:
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_OTLP_ENDPOINT, insecure=True))
    )
tracer = trace.get_tracer("audit-svc")

REQUEST_COUNT = Counter(
    "http_requests_total", "Peticiones HTTP recibidas", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Duracion de las peticiones HTTP", ["method", "path"]
)
from prometheus_client import Gauge
AUDIT_ENTRIES = Gauge("audit_entries_total", "Registros acumulados de auditoria")

DB_DSN = os.environ.get("DB_DSN", "postgresql://audit:audit@audit-db:5432/audit")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("audit-svc")

app = FastAPI(title="audit-svc")
FastAPIInstrumentor.instrument_app(app)
pool = None
nc = None
ready = {"db": False, "nats": False}


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - start
    path = request.url.path
    REQUEST_COUNT.labels(method=request.method, path=path, status=response.status_code).inc()
    REQUEST_LATENCY.labels(method=request.method, path=path).observe(duration)
    return response

SUBJECTS = ["gitea.push", "gitea.issue.created", "gitea.issue.closed", "gitea.pull_request.merged"]


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "audit-svc")
    log.info(json.dumps(kwargs))


async def handle_event(msg):
    try:
        envelope = json.loads(msg.data.decode())
        if not isinstance(envelope, dict):
            raise ValueError("invalid envelope")
        carrier = envelope.get("trace_context", {})
        ctx = propagate.extract(carrier if isinstance(carrier, dict) else {})
    except Exception:
        jlog(level="error", message="evento invalido, no ack")
        return
    with tracer.start_as_current_span("audit-svc.handle_event", context=ctx,
                                      record_exception=False) as span:
        await _handle_event_impl(msg, envelope)


async def _handle_event_impl(msg, envelope):
    request_id = envelope.get("request_id")
    if not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", request_id):
        request_id = None
    span = trace.get_current_span()
    correlation = {"request_id": request_id,
                   "trace_id": f"{span.get_span_context().trace_id:032x}"}
    try:
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
            jlog(level="info", message="registro guardado", **correlation)
        else:
            jlog(level="info", message="evento duplicado, ignorado (idempotencia)", **correlation)
        await msg.ack()
    except Exception:
        span.set_status(Status(StatusCode.ERROR))
        jlog(level="error", message="fallo al procesar evento, no ack", **correlation)


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
    AUDIT_ENTRIES.set(total)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


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
