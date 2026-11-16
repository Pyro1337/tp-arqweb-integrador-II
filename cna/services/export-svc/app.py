import os
import json
import uuid
import logging
import sys
import time
from datetime import datetime, timezone

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, CONTENT_TYPE_LATEST, generate_latest

REDIS_URL = os.environ.get("REDIS_URL", "redis://export-redis:6379/0")

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("export-svc-api")

REQUEST_COUNT = Counter(
    "http_requests_total", "Peticiones HTTP recibidas", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Duracion de las peticiones HTTP", ["method", "path"]
)
EXPORT_JOBS = Gauge("export_jobs_total", "Jobs de exportacion acumulados (claves en redis)")

app = FastAPI(title="export-svc")
r = None


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - start
    path = request.url.path
    REQUEST_COUNT.labels(method=request.method, path=path, status=response.status_code).inc()
    REQUEST_LATENCY.labels(method=request.method, path=path).observe(duration)
    return response


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "export-svc")
    log.info(json.dumps(kwargs))


class ExportRequest(BaseModel):
    repo: str
    format: str
    scope: str = "activity"


@app.on_event("startup")
async def startup():
    global r
    r = redis.from_url(REDIS_URL, decode_responses=True)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    try:
        await r.ping()
    except Exception:
        raise HTTPException(status_code=503, detail="redis no disponible")
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    total = await r.dbsize()
    EXPORT_JOBS.set(total)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v2/exports", status_code=202)
async def create_export(req: ExportRequest):
    if req.format not in ("csv", "pdf"):
        raise HTTPException(status_code=400, detail="formato invalido")
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    job = {"job_id": job_id, "status": "queued", "repo": req.repo, "format": req.format, "scope": req.scope}
    await r.set(f"job:{job_id}", json.dumps(job))
    await r.lpush("export_queue", job_id)
    jlog(level="info", message="job encolado", job_id=job_id)
    return job


@app.get("/v2/exports/{job_id}")
async def get_export(job_id: str):
    raw = await r.get(f"job:{job_id}")
    if not raw:
        raise HTTPException(status_code=404, detail="job no encontrado")
    return json.loads(raw)
