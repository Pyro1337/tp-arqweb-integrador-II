import os
import io
import json
import csv
import logging
import sys
import signal
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import redis
import httpx
from minio import Minio
from queue_helpers import IO_TIMEOUT, CLAIM_IDLE_MS, POLL_MS, MAX_ATTEMPTS, StreamQueue
from token_source import gitea_headers

REDIS_URL = os.environ.get("REDIS_URL", "redis://export-redis:6379/0")
GITEA_URL = os.environ.get("GITEA_URL", "http://host.docker.internal:3000")
GITEA_TOKEN = os.environ.get("GITEA_TOKEN", "")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_PUBLIC_ENDPOINT = os.environ.get("MINIO_PUBLIC_ENDPOINT", MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
BUCKET = "exports"
# Preserve main's opt-in component/chaos-test delay while keeping the lease live.
TEST_DELAY = float(os.environ.get("EXPORT_WORKER_TEST_DELAY", "0"))

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("export-worker")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "export-worker")
    log.info(json.dumps(kwargs))


def get_minio_client(public=False):
    from urllib3 import PoolManager, Timeout
    return Minio(
        MINIO_PUBLIC_ENDPOINT if public else MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY,
        secure=False, region="us-east-1",
        http_client=PoolManager(timeout=Timeout(connect=IO_TIMEOUT, read=IO_TIMEOUT), retries=False),
    )


def get_minio_public_client():
    # Region is explicit: signing never probes the public endpoint.
    return get_minio_client(public=True)


def handle_delivery(queue, client, message_id, fields, public_client=None):
    job = queue.begin(message_id, fields)
    if job is None:
        return
    with lease_heartbeat(queue, message_id, job):
        jlog(event="export_started", job_id=job["job_id"],
             worker=queue.consumer, attempts=job.get("attempts", 0))
        if job.get("format") != "csv":
            queue.finish(message_id, job, "failed", error="unsupported_format")
            return
        try:
            headers = gitea_headers(GITEA_TOKEN)
            resp = httpx.get(f"{GITEA_URL}/api/v1/repos/{job['repo']}/issues", headers=headers, timeout=IO_TIMEOUT)
            if resp.status_code != 200:
                raise ValueError("upstream status")
            issues = resp.json()
            if not isinstance(issues, list) or not all(isinstance(item, dict) for item in issues):
                raise ValueError("upstream shape")
        except Exception:
            queue.finish(message_id, job, "retry", error="upstream_unavailable")
            jlog(event="export_retry", job_id=job["job_id"], error="upstream_unavailable")
            return
        if TEST_DELAY:
            time.sleep(TEST_DELAY)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "title", "state", "created_at"])
        for issue in issues:
            writer.writerow([issue.get("number"), issue.get("title"), issue.get("state"), issue.get("created_at")])
        data = buf.getvalue().encode("utf-8")
        object_name = f"{job['job_id']}/{job['lease_token']}.csv"
        try:
            client.put_object(BUCKET, object_name, io.BytesIO(data), length=len(data), content_type="text/csv")
            public_client = public_client or get_minio_public_client()
            url = public_client.presigned_get_object(BUCKET, object_name)
        except Exception:
            queue.finish(message_id, job, "retry", error="storage_unavailable")
            jlog(event="export_retry", job_id=job["job_id"], error="storage_unavailable")
            return
        if queue.finish(message_id, job, "done", download_url=url, object_name=object_name):
            jlog(event="export_done", job_id=job["job_id"], worker=queue.consumer)




@contextmanager
def lease_heartbeat(queue, message_id, job):
    stopped = threading.Event()
    interval = max(.005, getattr(queue, 'claim_idle_ms', CLAIM_IDLE_MS) / 3000)
    def renew():
        while not stopped.wait(interval):
            try:
                if not queue.renew(message_id, job):
                    return
            except Exception:
                # No unsafe writes on failure; final commit is always fenced.
                return
    thread = threading.Thread(target=renew, name='export-lease-' + job['lease_token'], daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=IO_TIMEOUT + 1)


def run_worker(connection, client, stopped):
    try:
        queue = StreamQueue(connection)
        jlog(event="worker_started", consumer=queue.consumer)
        while not stopped.is_set():
            try:
                item = queue.next_delivery()
                if item:
                    handle_delivery(queue, client, *item)
            except Exception:
                # Redis/state failures are NOT acknowledged. No traceback/URL.
                jlog(event="worker_retry", error="dependency_unavailable")
                stopped.wait(POLL_MS / 1000)
    finally:
        connection.close()
        client._http.clear()


def main():
    stopped = threading.Event()
    def cancel(signum, frame):
        stopped.set()
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, cancel)
    signal.signal(signal.SIGINT, cancel)
    connection = client = None
    try:
        connection = redis.from_url(REDIS_URL, decode_responses=True,
                                    socket_timeout=max(IO_TIMEOUT, POLL_MS / 1000 + 1),
                                    socket_connect_timeout=IO_TIMEOUT)
        client = get_minio_client()
        # Deployment is responsible for credentials; bucket creation retains baseline behavior.
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)
        run_worker(connection, client, stopped)
    except KeyboardInterrupt:
        pass
    except Exception:
        jlog(event="worker_startup_failed", error="dependency_unavailable")
        raise SystemExit(1) from None
    finally:
        if connection is not None:
            connection.close()
        if client is not None:
            client._http.clear()


if __name__ == "__main__":
    main()
