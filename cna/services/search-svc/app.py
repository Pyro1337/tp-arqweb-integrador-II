import os
import json
import logging
import sys
import asyncio
import time
import re
from uuid import uuid4
from datetime import datetime, timezone

import httpx
import nats
from resilience import GiteaResilience, wait_for_task
from token_source import gitea_headers

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

JAEGER_OTLP_ENDPOINT = os.environ.get("JAEGER_OTLP_ENDPOINT", "jaeger:4317")
trace.set_tracer_provider(TracerProvider(resource=Resource.create({"service.name": "search-svc"})))
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_OTLP_ENDPOINT, insecure=True))
)
tracer = trace.get_tracer("search-svc")

REQUEST_COUNT = Counter(
    "http_requests_total", "Peticiones HTTP recibidas", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Duracion de las peticiones HTTP", ["method", "path"]
)

GITEA_URL = os.environ.get("GITEA_URL", "http://host.docker.internal:3000")
GITEA_TOKEN = os.environ.get("GITEA_TOKEN", "")
MEILI_URL = os.environ.get("MEILI_URL", "http://meilisearch:7700")
MEILI_KEY = os.environ.get("MEILI_KEY", "masterKey123")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
INDEX = "gitea_content"
MEILI_TASK_TIMEOUT = 4.0
# Bound queueing + upstream retries + indexing/polling + ACK below ack_wait=10.
HANDLER_TIMEOUT = 9.0

# timeout corto a proposito: si Gitea tarda mas que esto (ej. con latencia de red
# artificial), queremos que falle rapido en vez de quedarse colgado ocupando el hilo
GITEA_TIMEOUT = float(os.environ.get("GITEA_TIMEOUT", "1.5"))
CIRCUIT_FAILURE_THRESHOLD = 3   # fallas consecutivas para abrir el circuito
CIRCUIT_COOLDOWN = 15           # segundos que el circuito queda abierto

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("search-svc")

app = FastAPI(title="search-svc")
FastAPIInstrumentor.instrument_app(app)
nc = None
ready = {"meili": False, "nats": False}
seen_events = set()


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - start
    path = request.url.path
    REQUEST_COUNT.labels(method=request.method, path=path, status=response.status_code).inc()
    REQUEST_LATENCY.labels(method=request.method, path=path).observe(duration)
    return response


gitea = GiteaResilience(request_timeout=GITEA_TIMEOUT,
                        failure_threshold=CIRCUIT_FAILURE_THRESHOLD, cooldown=CIRCUIT_COOLDOWN)


def correlation_headers(request_id):
    headers = {"X-Request-ID": request_id} if request_id else {}
    propagate.inject(headers)
    return headers


async def get_repo_con_reintentos(client, repo, request_id=None):
    # Resolve inside each attempt; an explicit invalid file must fail closed.
    def headers():
        return {**correlation_headers(request_id), **gitea_headers(GITEA_TOKEN)}

    return await gitea.get(client, f"{GITEA_URL}/api/v1/repos/{repo}", headers=headers)


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "search-svc")
    log.info(json.dumps(kwargs))


async def meili_headers():
    return {"Authorization": f"Bearer {MEILI_KEY}", "Content-Type": "application/json"}


async def ensure_index():
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{MEILI_URL}/indexes", json={"uid": INDEX, "primaryKey": "id"},
                               headers=await meili_headers(), timeout=5)
            ready["meili"] = True
        except Exception as e:
            jlog(level="error", message=f"no se pudo crear indice: {e}")


async def handle_push(msg):
    try:
        envelope = json.loads(msg.data.decode())
        if not isinstance(envelope, dict):
            raise ValueError("invalid event envelope")
    except (UnicodeError, ValueError):
        jlog(level="error", message="sobre de evento invalido, no ack")
        return
    # continuamos la misma traza que arranco event-bridge, usando el traceparent
    # que viajo adentro del sobre del evento
    legacy = envelope.get("headers")
    legacy = {str(k).lower(): v for k, v in legacy.items()} if isinstance(legacy, dict) else {}
    request_id = (envelope.get("request_id") or envelope.get("X-Request-ID")
                  or legacy.get("x-request-id"))
    if not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", request_id):
        request_id = uuid4().hex
    envelope["request_id"] = request_id
    carrier = envelope.get("trace_context")
    if not isinstance(carrier, dict):
        carrier = {key: envelope.get(key) or legacy.get(key)
                   for key in ("traceparent", "tracestate")}
    ctx = propagate.extract(carrier)
    with tracer.start_as_current_span("search-svc.handle_push", context=ctx,
                                      record_exception=False, set_status_on_exception=False) as span:
        try:
            await asyncio.wait_for(_handle_push_impl(msg, envelope), HANDLER_TIMEOUT)
        except asyncio.TimeoutError:
            span.set_status(Status(StatusCode.ERROR))
            jlog(level="error", message="plazo de procesamiento vencido, no ack")


async def _handle_push_impl(msg, envelope):
    try:
        event_id = envelope["event_id"]
        if event_id in seen_events:
            await msg.ack()
            return

        repo = envelope["payload"]["repo"]
        request_id = envelope.get("request_id")

        with tracer.start_as_current_span("search-svc.consultar_gitea_api", record_exception=False,
                                          set_status_on_exception=False):
            async with httpx.AsyncClient() as client:
                resp = await get_repo_con_reintentos(client, repo, request_id)
                data = resp.json()

        doc = {
            "id": repo.replace("/", "_"),
            "type": "repo",
            "title": data.get("name", repo),
            "repo": repo,
            "url": data.get("html_url", f"{GITEA_URL}/{repo}"),
            "description": data.get("description", ""),
        }
        with tracer.start_as_current_span("search-svc.indexar_meilisearch", record_exception=False,
                                          set_status_on_exception=False):
            async with httpx.AsyncClient() as client:
                headers = {**(await meili_headers()), **correlation_headers(request_id)}
                response = await client.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=[doc],
                                             headers=headers, timeout=5)
                response.raise_for_status()
                task_uid = response.json()["taskUid"]
                await wait_for_task(client, f"{MEILI_URL}/tasks/{task_uid}",
                                    headers, MEILI_TASK_TIMEOUT)

        await msg.ack()
        seen_events.add(event_id)
        jlog(level="info", message="reindexado", repo=repo, event_id=event_id, request_id=request_id)
    except Exception:
        trace.get_current_span().set_status(Status(StatusCode.ERROR))
        jlog(level="error", message="fallo al procesar evento, no ack")
        # no hacemos ack -> queda para reintento / DLQ segun configuracion del stream


@app.on_event("startup")
async def startup():
    global nc
    await ensure_index()
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    for attempt in range(10):
        try:
            await js.subscribe(
                "gitea.push", durable="search-svc", cb=handle_push, manual_ack=True,
                config=ConsumerConfig(max_deliver=3, ack_wait=10),
            )
            ready["nats"] = True
            jlog(level="info", message="suscripto a gitea.push")
            break
        except Exception as e:
            jlog(level="warning", message=f"stream EVENTS aun no listo, reintento {attempt + 1}/10: {e}")
            await asyncio.sleep(2)
    else:
        jlog(level="error", message="no se pudo suscribir a gitea.push tras 10 intentos")


@app.on_event("shutdown")
async def shutdown():
    global nc
    connection, nc = nc, None
    ready.update(meili=False, nats=False)
    if connection is not None and not connection.is_closed:
        await connection.close()


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    if not ready["meili"] or not ready["nats"]:
        raise HTTPException(status_code=503, detail="dependencias no disponibles")
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v2/search")
async def search(q: str):
    if not q:
        raise HTTPException(status_code=400, detail="falta el parametro q")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"{MEILI_URL}/indexes/{INDEX}/search", json={"q": q},
                                      headers=await meili_headers(), timeout=5)
        except Exception:
            raise HTTPException(status_code=503, detail="indice de busqueda no disponible")
    if resp.status_code != 200:
        raise HTTPException(status_code=503, detail="indice de busqueda no disponible")
    hits = resp.json().get("hits", [])
    return {"query": q, "total": len(hits), "results": hits}
