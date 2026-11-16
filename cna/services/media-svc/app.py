import os
import io
import json
import uuid
import logging
import sys
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import Response
from minio import Minio
from PIL import Image
import nats
from prometheus_client import Counter, Histogram, CONTENT_TYPE_LATEST, generate_latest

REQUEST_COUNT = Counter(
    "http_requests_total", "Peticiones HTTP recibidas", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Duracion de las peticiones HTTP", ["method", "path"]
)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_PUBLIC_ENDPOINT = os.environ.get("MINIO_PUBLIC_ENDPOINT", MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
BUCKET = "media"

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("media-svc")

app = FastAPI(title="media-svc")
client = None
public_client = None
nc = None


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
    kwargs.setdefault("service", "media-svc")
    log.info(json.dumps(kwargs))


@app.on_event("startup")
async def startup():
    global client, public_client, nc
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    public_client = Minio(
        MINIO_PUBLIC_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY,
        secure=False, region="us-east-1",
    )
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    nc = await nats.connect(NATS_URL)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    try:
        client.bucket_exists(BUCKET)
    except Exception:
        raise HTTPException(status_code=503, detail="minio no disponible")
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v2/media", status_code=202)
async def upload_media(file: UploadFile = File(...), repo: str = Form(...), ref_type: str = Form("issue"), ref_id: int = Form(0)):
    media_id = uuid.uuid4().hex[:12]
    content = await file.read()

    client.put_object(BUCKET, f"{media_id}/original", io.BytesIO(content), length=len(content))

    try:
        img = Image.open(io.BytesIO(content))
        img.thumbnail((200, 200))
        thumb_buf = io.BytesIO()
        img.save(thumb_buf, format=img.format or "PNG")
        thumb_bytes = thumb_buf.getvalue()
        client.put_object(BUCKET, f"{media_id}/thumbnail", io.BytesIO(thumb_bytes), length=len(thumb_bytes))
        status = "ready"
    except Exception as e:
        jlog(level="warning", message=f"no se pudo generar miniatura: {e}")
        status = "ready_no_thumbnail"

    envelope = {
        "event_id": str(uuid.uuid4()),
        "type": "media.uploaded",
        "version": "v1",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "request_id": media_id,
        "payload": {"media_id": media_id, "repo": repo, "ref_type": ref_type},
    }
    await nc.publish("media.uploaded", json.dumps(envelope).encode())
    jlog(level="info", message="archivo subido", media_id=media_id, repo=repo)

    return {"id": media_id, "url": public_client.presigned_get_object(BUCKET, f"{media_id}/original"), "status": status}


@app.get("/v2/media/{media_id}")
async def get_media(media_id: str):
    try:
        url = public_client.presigned_get_object(BUCKET, f"{media_id}/original")
        thumb_url = public_client.presigned_get_object(BUCKET, f"{media_id}/thumbnail")
    except Exception:
        raise HTTPException(status_code=404, detail="no encontrado")
    return {"id": media_id, "url": url, "thumbnail_url": thumb_url, "status": "ready"}
