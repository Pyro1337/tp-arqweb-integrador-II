import os
import io
import json
import csv
import socket
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
# solo para chaos testing (X3): simula un job que tarda, para poder matar el worker
# a mitad de camino y verificar que otro lo retoma. 0 = sin demora (comportamiento normal)
TEST_DELAY = float(os.environ.get("EXPORT_WORKER_TEST_DELAY", "0"))

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


WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
HEARTBEAT_TTL = 15  # segundos: si un worker muere a mitad de un job, el heartbeat
                     # expira y otro worker lo detecta como huerfano y lo reencola


def process_job(r, client, job_id):
    raw = r.get(f"job:{job_id}")
    if not raw:
        return
    job = json.loads(raw)
    job["status"] = "running"
    job["worker"] = WORKER_ID
    r.set(f"job:{job_id}", json.dumps(job))
    r.set(f"job:heartbeat:{job_id}", WORKER_ID, ex=HEARTBEAT_TTL)
    jlog(level="info", message="procesando job", job_id=job_id, worker=WORKER_ID)

    try:
        headers = {"Authorization": f"token {GITEA_TOKEN}"} if GITEA_TOKEN else {}
        resp = httpx.get(f"{GITEA_URL}/api/v1/repos/{job['repo']}/issues", headers=headers, timeout=15)
        issues = resp.json() if resp.status_code == 200 else []

        if TEST_DELAY:
            jlog(level="info", message=f"demora de prueba de {TEST_DELAY}s antes de terminar", job_id=job_id)
            time.sleep(TEST_DELAY)

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
        jlog(level="info", message="job completado", job_id=job_id, worker=WORKER_ID)
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        r.set(f"job:{job_id}", json.dumps(job))
        jlog(level="error", message=f"job fallido: {e}", job_id=job_id, worker=WORKER_ID)
    finally:
        r.lrem("export_processing", 0, job_id)
        r.delete(f"job:heartbeat:{job_id}")


def reencolar_huerfanos(r):
    # cualquier worker, antes de tomar un job nuevo, revisa si quedo algun job
    # "en proceso" cuyo heartbeat ya vencio (el worker que lo tenia murio a mitad
    # de camino) y lo devuelve a la cola principal para que otro lo retome.
    for job_id in r.lrange("export_processing", 0, -1):
        if not r.exists(f"job:heartbeat:{job_id}"):
            if r.lrem("export_processing", 1, job_id):
                r.rpush("export_queue", job_id)
                jlog(level="warning", message="job huerfano reencolado", job_id=job_id, worker=WORKER_ID)


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    client = get_minio_client()
    jlog(level="info", message="worker iniciado", worker=WORKER_ID)
    while True:
        reencolar_huerfanos(r)
        item = r.brpoplpush("export_queue", "export_processing", timeout=5)
        if item is None:
            continue
        process_job(r, client, item)


if __name__ == "__main__":
    main()
