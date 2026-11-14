import os
import io
import json
import csv
import time
import logging
import sys
from datetime import datetime, timezone

import redis
import httpx
from minio import Minio
from minio.error import S3Error

REDIS_URL = os.environ.get("REDIS_URL", "redis://export-redis:6379/0")
GITEA_URL = os.environ.get("GITEA_URL", "http://host.docker.internal:3000")
GITEA_TOKEN = os.environ.get("GITEA_TOKEN", "")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
# endpoint publico (IP:puerto de VM-2) para que las URLs prefirmadas sirvan desde afuera de la red de docker
MINIO_PUBLIC_ENDPOINT = os.environ.get("MINIO_PUBLIC_ENDPOINT", MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = "exports"

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("export-worker")


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "export-worker")
    log.info(json.dumps(kwargs))


def get_minio_client():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    return client


def get_minio_public_client():
    # mismo bucket, pero apuntando al endpoint publico solo para firmar URLs.
    # fijamos la region a mano para que el SDK no intente auto-detectarla haciendo
    # una consulta de red hacia su propia IP publica (eso colgaba el worker).
    return Minio(
        MINIO_PUBLIC_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY,
        secure=False, region="us-east-1",
    )


def process_job(r, client, job_id):
    raw = r.get(f"job:{job_id}")
    if not raw:
        return
    job = json.loads(raw)
    job["status"] = "running"
    r.set(f"job:{job_id}", json.dumps(job))
    jlog(level="info", message="procesando job", job_id=job_id)

    try:
        headers = {"Authorization": f"token {GITEA_TOKEN}"} if GITEA_TOKEN else {}
        resp = httpx.get(f"{GITEA_URL}/api/v1/repos/{job['repo']}/issues", headers=headers, timeout=15)
        issues = resp.json() if resp.status_code == 200 else []

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "title", "state", "created_at"])
        for issue in issues:
            writer.writerow([issue.get("number"), issue.get("title"), issue.get("state"), issue.get("created_at")])
        data = buf.getvalue().encode("utf-8")

        object_name = f"{job_id}.csv"
        client.put_object(BUCKET, object_name, io.BytesIO(data), length=len(data), content_type="text/csv")
        public_client = get_minio_public_client()
        url = public_client.presigned_get_object(BUCKET, object_name)

        job["status"] = "done"
        job["download_url"] = url
        r.set(f"job:{job_id}", json.dumps(job))
        jlog(level="info", message="job completado", job_id=job_id)
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        r.set(f"job:{job_id}", json.dumps(job))
        jlog(level="error", message=f"job fallido: {e}", job_id=job_id)


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    client = get_minio_client()
    jlog(level="info", message="worker iniciado")
    while True:
        item = r.brpop("export_queue", timeout=5)
        if item is None:
            continue
        _, job_id = item
        process_job(r, client, job_id)


if __name__ == "__main__":
    main()
