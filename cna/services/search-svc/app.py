import os
import json
import logging
import sys
import asyncio
import time
from datetime import datetime, timezone

import httpx
import nats
from nats.js.api import ConsumerConfig
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, CONTENT_TYPE_LATEST, generate_latest

from opentelemetry import trace, propagate
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


class CircuitBreaker:
    """Circuit breaker simple: tras N fallas seguidas, se 'abre' y rechaza
    llamadas sin siquiera intentarlas durante COOLDOWN segundos, para no
    saturar hilos/conexiones contra un servicio que ya sabemos que esta lento o caido."""

    def __init__(self, threshold, cooldown):
        self.threshold = threshold
        self.cooldown = cooldown
        self.failures = 0
        self.opened_at = None

    def is_open(self):
        if self.opened_at is None:
            return False
        if time.monotonic() - self.opened_at >= self.cooldown:
            # paso el cooldown: dejamos pasar un intento de prueba (half-open)
            self.opened_at = None
            self.failures = 0
            return False
        return True

    def record_success(self):
        self.failures = 0
        self.opened_at = None

    def record_failure(self):
        self.failures += 1
        if self.failures >= self.threshold:
            self.opened_at = time.monotonic()


gitea_circuit = CircuitBreaker(CIRCUIT_FAILURE_THRESHOLD, CIRCUIT_COOLDOWN)


async def get_repo_con_reintentos(client, repo):
    """Llama a la API de Gitea con timeout corto, backoff exponencial (3 intentos:
    0.5s, 1s, 2s de espera entre reintentos) y circuit breaker."""
    if gitea_circuit.is_open():
        jlog(level="warning", message="circuito abierto, no se intenta llamar a Gitea", repo=repo)
        raise RuntimeError("circuit_open")

    headers = {"Authorization": f"token {GITEA_TOKEN}"} if GITEA_TOKEN else {}
    delays = [0.5, 1, 2]
    last_exc = None
    for attempt, delay in enumerate([0] + delays):
        if delay:
            await asyncio.sleep(delay)
        try:
            resp = await client.get(f"{GITEA_URL}/api/v1/repos/{repo}", headers=headers, timeout=GITEA_TIMEOUT)
            gitea_circuit.record_success()
            return resp
        except Exception as e:
            last_exc = e
            jlog(level="warning", message=f"timeout/error llamando a Gitea, intento {attempt + 1}: {e}", repo=repo)
    gitea_circuit.record_failure()
    raise last_exc


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
    envelope = json.loads(msg.data.decode())
    # continuamos la misma traza que arranco event-bridge, usando el traceparent
    # que viajo adentro del sobre del evento
    ctx = propagate.extract(envelope.get("trace_context", {}))
    with tracer.start_as_current_span("search-svc.handle_push", context=ctx):
        await _handle_push_impl(msg, envelope)


async def _handle_push_impl(msg, envelope):
    try:
        event_id = envelope["event_id"]
        if event_id in seen_events:
            await msg.ack()
            return
        seen_events.add(event_id)

        repo = envelope["payload"]["repo"]
        request_id = envelope.get("request_id")

        with tracer.start_as_current_span("search-svc.consultar_gitea_api"):
            async with httpx.AsyncClient() as client:
                resp = await get_repo_con_reintentos(client, repo)
                data = resp.json() if resp.status_code == 200 else {}

        doc = {
            "id": repo.replace("/", "_"),
            "type": "repo",
            "title": data.get("name", repo),
            "repo": repo,
            "url": data.get("html_url", f"{GITEA_URL}/{repo}"),
            "description": data.get("description", ""),
        }
        with tracer.start_as_current_span("search-svc.indexar_meilisearch"):
            async with httpx.AsyncClient() as client:
                await client.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=[doc],
                                   headers=await meili_headers(), timeout=5)

        jlog(level="info", message="reindexado", repo=repo, event_id=event_id, request_id=request_id)
        await msg.ack()
    except Exception as e:
        jlog(level="error", message=f"fallo al procesar evento, no ack: {e}")
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
