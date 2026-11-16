"""UNIT uses protocol fake, NOT broker durability proof. Integration uses an
explicit ARQWEB_REDIS_SERVER private fixture on a Unix socket, never TCP.
"""
import json
import pytest
import httpx
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
import app as api
import worker




class UnitQueue:
    """Only tests worker orchestration; does NOT emulate Redis or prove Lua."""
    consumer = "new-consumer"
    def __init__(self):
        self.job = {"job_id": "job-1234abcd", "repo": "team/repo", "format": "csv",
                    "request_id": "safe-request", "trace_context": {}, "attempts": 1,
                    "lease_token": "attempt-token"}
        self.finished = []
    def begin(self, message_id, fields):
        return self.job.copy()
    def finish(self, message_id, job, status, **extra):
        self.finished.append((status, extra))
        return True
    def renew(self, *args):
        return True


class UnitStore:
    def __init__(self):
        self.objects = {}
    def put_object(self, bucket, name, data, **kwargs):
        self.objects[name] = data.read()
    def presigned_get_object(self, bucket, name):
        return "https://download.invalid/" + name


def test_unit_claimed_job_completes_csv(monkeypatch):
    queue, store = UnitQueue(), UnitStore()
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: __import__('httpx').Response(
        200, json=[{"number": 7, "title": "Hello", "state": "open", "created_at": "today"}]))
    assert hasattr(worker, "handle_delivery"), "worker must process acknowledged stream deliveries"
    worker.handle_delivery(queue, store, "1-0", {"job_id": queue.job["job_id"]}, public_client=store)
    assert queue.finished[0][0] == "done"
    name, data = next(iter(store.objects.items()))
    assert "attempt-token" in name
    assert data == b"id,title,state,created_at\r\n7,Hello,open,today\r\n"


@pytest.mark.parametrize("response", [httpx.Response(503, json=[]), httpx.Response(200, content=b"not-json")])
def test_unit_upstream_failure_never_done(monkeypatch, response, caplog):
    queue, store = UnitQueue(), UnitStore()
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: response)
    worker.handle_delivery(queue, store, "1-0", {"job_id": queue.job["job_id"]}, public_client=store)
    assert not store.objects
    assert queue.finished[0][0] == "retry"
    assert queue.finished[0][1]["error"] == "upstream_unavailable"
    assert "not-json" not in caplog.text


import os
import subprocess
import time
import redis
import queue_helpers as qh


@pytest.fixture
def broker(tmp_path):
    binary = os.environ.get("ARQWEB_REDIS_SERVER")
    if not binary:
        pytest.skip("real Redis requires explicit ARQWEB_REDIS_SERVER")
    sock = str(tmp_path / "redis.sock")
    proc = subprocess.Popen([binary, "--port", "0", "--unixsocket", sock,
                             "--unixsocketperm", "700", "--save", "", "--appendonly", "no",
                             "--maxmemory", "32mb", "--maxmemory-policy", "noeviction"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    watchdog = threading.Timer(15, proc.kill)
    watchdog.start()
    connection = redis.Redis(unix_socket_path=sock, decode_responses=True, socket_timeout=1)
    try:
        deadline = time.monotonic() + 3
        while True:
            try:
                connection.ping()
                break
            except redis.ConnectionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.01)
        assert connection.config_get("port")["port"] == "0"
        assert connection.config_get("maxmemory")["maxmemory"] == str(32 * 1024 * 1024)
        assert connection.config_get("maxmemory-policy")["maxmemory-policy"] == "noeviction"
        yield connection
    finally:
        connection.close()
        watchdog.cancel()
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
        watchdog.join(timeout=1)
        assert not watchdog.is_alive()
        print("PRIVATE_REDIS_CLEANUP", json.dumps({"pid": proc.pid, "returncode": proc.returncode,
              "reaped": proc.poll() is not None, "port": 0, "maxmemory": "32mb"}))


def enqueue(broker, **changes):
    job = {"job_id": "job-1234abcd", "repo": "team/repo", "format": "csv", "status": "queued",
           "attempts": 0, "request_id": "integration-request", "trace_context": {}}
    job.update(changes)
    message_id = broker.eval(qh.CREATE_JOB, 4, "job:" + job["job_id"], qh.STREAM,
                             qh.LEGACY_QUEUE, qh.LEGACY_PROCESSING,
                             job["job_id"], json.dumps(job))
    return job, message_id


def test_real_redis_crashed_owner_reclaimed_and_fenced(broker, monkeypatch):
    assert hasattr(qh, "StreamQueue"), "durable consumer group recovery is missing"
    old = qh.StreamQueue(broker, consumer="dead-worker", claim_idle_ms=10, poll_ms=1)
    new = qh.StreamQueue(broker, consumer="new-worker", claim_idle_ms=10, poll_ms=1)
    job, message_id = enqueue(broker)
    delivery = old.next_delivery()
    old_job = old.begin(*delivery)
    assert old_job["status"] == "running"
    assert old_job["worker"] == old.consumer, "main API worker field must remain available"
    # Simulate process crash: pending delivery remains, no ACK or heartbeat.
    broker.xclaim(qh.STREAM, qh.GROUP, "dead-worker", 0, [message_id], idle=11, justid=True)
    assert new.next_delivery()[0] == message_id
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: httpx.Response(200, json=[]))
    store = UnitStore()
    worker.handle_delivery(new, store, message_id, delivery[1], public_client=store)
    winner = json.loads(broker.get("job:" + job["job_id"]))
    assert winner["status"] == "done"
    assert winner["attempts"] == 2
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0
    assert not old.finish(message_id, old_job, "done", download_url="stale")
    assert json.loads(broker.get("job:" + job["job_id"])) == winner
    assert old_job["lease_token"] not in winner["object_name"]



def test_real_retry_budget_no_early_ack(broker, monkeypatch):
    queue = qh.StreamQueue(broker, consumer="retry-worker", claim_idle_ms=10, poll_ms=1, max_attempts=2)
    job, message_id = enqueue(broker)
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: httpx.Response(503, json=[]))
    store = UnitStore()
    worker.handle_delivery(queue, store, *queue.next_delivery(), public_client=store)
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 1
    assert json.loads(broker.get("job:" + job["job_id"]))["status"] == "retry"
    broker.xclaim(qh.STREAM, qh.GROUP, queue.consumer, 0, [message_id], idle=11, justid=True)
    worker.handle_delivery(queue, store, *queue.next_delivery(), public_client=store)
    result = json.loads(broker.get("job:" + job["job_id"]))
    assert result["status"] == "failed", "retry budget must reach durable final failure"
    assert result["attempts"] == 2
    assert result["error"] == "upstream_unavailable"
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0
    assert not store.objects


def test_real_completed_replay_only_acks(broker):
    queue = qh.StreamQueue(broker, consumer="replay-worker", poll_ms=1)
    job, message_id = enqueue(broker, status="done", download_url="winner", attempts=1)
    delivery = queue.next_delivery()
    before = broker.get("job:" + job["job_id"])
    worker.handle_delivery(queue, UnitStore(), *delivery)
    assert broker.get("job:" + job["job_id"]) == before
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0


@pytest.mark.parametrize("raw", [None, "{bad-json", "[]", '{"attempts":"bad"}'])
def test_real_missing_or_malformed_job_durably_failed(broker, raw):
    queue = qh.StreamQueue(broker, consumer="safe-worker", poll_ms=1)
    job, message_id = enqueue(broker)
    if raw is None:
        broker.delete("job:" + job["job_id"])
    else:
        broker.set("job:" + job["job_id"], raw)
    worker.handle_delivery(queue, UnitStore(), *queue.next_delivery())
    result = json.loads(broker.get("job:" + job["job_id"]))
    assert result["status"] == "failed"
    assert result["error"] == "invalid_job_metadata"
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0



def test_real_heartbeat_only_current_owner_renews(broker):
    old = qh.StreamQueue(broker, consumer="old", claim_idle_ms=100, poll_ms=1)
    new = qh.StreamQueue(broker, consumer="new", claim_idle_ms=100, poll_ms=1)
    job, message_id = enqueue(broker)
    delivery = old.next_delivery()
    old_job = old.begin(*delivery)
    broker.xclaim(qh.STREAM, qh.GROUP, "old", 0, [message_id], idle=101, justid=True)
    assert hasattr(old, "renew"), "live work must renew its lease"
    assert old.renew(message_id, old_job)
    assert new.next_delivery() is None
    broker.xclaim(qh.STREAM, qh.GROUP, "new", 0, [message_id], idle=0, justid=True)
    new_job = new.begin(message_id, delivery[1])
    assert not old.renew(message_id, old_job)
    assert not old.finish(message_id, old_job, "failed", error="stale")
    assert new.finish(message_id, new_job, "done", object_name="winner")


def test_unit_upload_failure_sanitized(monkeypatch, caplog):
    queue, store = UnitQueue(), UnitStore()
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: httpx.Response(200, json=[]))
    def fail(*a, **k):
        raise RuntimeError("https://user:secret@private.invalid/token")
    store.put_object = fail
    worker.handle_delivery(queue, store, "1-0", {"job_id": queue.job["job_id"]}, public_client=store)
    assert queue.finished == [("retry", {"error": "storage_unavailable"})]
    assert "secret" not in caplog.text


def test_unit_pdf_is_explicit_failure(monkeypatch):
    queue, store = UnitQueue(), UnitStore()
    queue.job["format"] = "pdf"
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: pytest.fail("PDF must not call upstream"))
    worker.handle_delivery(queue, store, "1-0", {"job_id": queue.job["job_id"]}, public_client=store)
    assert queue.finished == [("failed", {"error": "unsupported_format"})]
    assert not store.objects


def test_real_malformed_stream_id_safe_failure(broker):
    queue = qh.StreamQueue(broker, consumer="safe", poll_ms=1)
    message_id = broker.xadd(qh.STREAM, {"job_id": "https://user:secret@invalid"})
    worker.handle_delivery(queue, UnitStore(), *queue.next_delivery())
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0
    raw = broker.get("job:invalid-" + message_id)
    assert json.loads(raw)["error"] == "invalid_job_metadata"
    assert "secret" not in raw


import threading


def test_unit_worker_heartbeat_stops_on_cancellation(monkeypatch):
    queue, store = UnitQueue(), UnitStore()
    queue.claim_idle_ms = 30
    renewed = threading.Event()
    queue.renew = lambda *a: renewed.set() or True
    def cancelled(*a, **k):
        assert renewed.wait(.5), "worker must renew while IO is in flight"
        raise KeyboardInterrupt()
    monkeypatch.setattr(worker.httpx, "get", cancelled)
    with pytest.raises(KeyboardInterrupt):
        worker.handle_delivery(queue, store, "1-0", {"job_id": queue.job["job_id"]}, public_client=store)
    assert not queue.finished, "cancellation must leave work pending"
    assert not any(t.name.startswith("export-lease-") for t in threading.enumerate())


def test_unit_shutdown_closes_clients(monkeypatch):
    from unittest.mock import Mock
    connection, store = Mock(), Mock()
    queue = Mock(consumer="test-consumer")
    queue.next_delivery.side_effect = KeyboardInterrupt
    assert hasattr(worker, "run_worker"), "worker loop and shutdown path missing"
    monkeypatch.setattr(worker, "StreamQueue", lambda *a, **k: queue)
    with pytest.raises(KeyboardInterrupt):
        worker.run_worker(connection, store, threading.Event())
    connection.close.assert_called_once()
    store._http.clear.assert_called_once()


def test_real_atomic_enqueue_rejects_bad_queue_without_record(broker):
    broker.set(qh.STREAM, "wrong-type")
    with pytest.raises(redis.ResponseError):
        enqueue(broker)
    assert broker.get("job:job-1234abcd") is None





@pytest.mark.parametrize("kwargs", [{"poll_ms": 0}, {"poll_ms": 10001}, {"claim_idle_ms": 0}, {"max_attempts": 0}])
def test_unit_invalid_unbounded_configuration_rejected(kwargs):
    from unittest.mock import Mock
    with pytest.raises(ValueError, match="invalid export queue configuration"):
        qh.StreamQueue(Mock(), **kwargs)


def test_unit_api_enqueue_failure_503_safe(monkeypatch):
    api.r = AsyncMock()
    api.r.eval.side_effect = RuntimeError("https://secret@redis.invalid")
    response = TestClient(api.app).post("/v2/exports", json={"repo": "team/repo", "format": "csv"})
    assert response.status_code == 503
    assert "secret" not in response.text



def test_unit_api_redis_timeouts_and_cleanup(monkeypatch):
    import asyncio
    from unittest.mock import Mock
    connection = AsyncMock()
    factory = Mock(return_value=connection)
    monkeypatch.setattr(api.redis, "from_url", factory)
    asyncio.run(api.startup())
    assert factory.call_args.kwargs.get("socket_timeout") == qh.IO_TIMEOUT
    assert factory.call_args.kwargs.get("socket_connect_timeout") == qh.IO_TIMEOUT
    assert hasattr(api, "shutdown"), "API Redis client needs explicit shutdown"
    asyncio.run(api.shutdown())
    connection.aclose.assert_awaited_once()


def test_unit_main_startup_error_is_sanitized(monkeypatch, caplog):
    from unittest.mock import Mock
    connection, store = Mock(), Mock()
    monkeypatch.setattr(worker.signal, "signal", lambda *a: None)
    monkeypatch.setattr(worker.redis, "from_url", lambda *a, **k: connection)
    monkeypatch.setattr(worker, "get_minio_client", lambda *a, **k: store)
    store.bucket_exists.side_effect = RuntimeError("https://user:secret@storage.invalid")
    with pytest.raises(SystemExit) as exc:
        worker.main()
    assert exc.value.code == 1
    assert "secret" not in caplog.text
    connection.close.assert_called()
    store._http.clear.assert_called()


@pytest.mark.parametrize("legacy", ["export_queue", "export_processing"])
def test_real_legacy_backlog_aborts_submit_and_worker_without_mutation(broker, legacy):
    broker.lpush(legacy, "job-abcdef12")
    broker.set("job:job-abcdef12", '{"status":"queued"}')
    before = broker.dump(legacy), broker.dump("job:job-abcdef12")
    with pytest.raises(redis.ResponseError, match="legacy export backlog"):
        enqueue(broker)
    assert broker.get("job:job-1234abcd") is None
    assert not broker.exists(qh.STREAM)
    with pytest.raises(redis.ResponseError, match="legacy export backlog"):
        qh.StreamQueue(broker, poll_ms=1)
    assert not broker.exists(qh.STREAM)
    assert (broker.dump(legacy), broker.dump("job:job-abcdef12")) == before


def test_real_stale_finish_before_reclaimed_owner_begins(broker):
    old = qh.StreamQueue(broker, consumer="old", poll_ms=1)
    new = qh.StreamQueue(broker, consumer="new", poll_ms=1)
    job, message_id = enqueue(broker)
    delivery = old.next_delivery()
    old_job = old.begin(*delivery)
    broker.xclaim(qh.STREAM, qh.GROUP, "new", 0, [message_id], justid=True)
    before = broker.get("job:" + job["job_id"])
    assert not old.finish(message_id, old_job, "done", download_url="stale")
    assert not old.renew(message_id, old_job)
    assert broker.get("job:" + job["job_id"]) == before
    new_job = new.begin(message_id, delivery[1])
    assert new_job["attempts"] == 2
    assert new.finish(message_id, new_job, "done", object_name="winner")
    winner = broker.get("job:" + job["job_id"])
    assert not new.finish(message_id, new_job, "failed", error="duplicate")
    assert broker.get("job:" + job["job_id"]) == winner


def test_real_abandoned_attempts_are_bounded(broker):
    queue = qh.StreamQueue(broker, consumer="worker", poll_ms=1, max_attempts=2)
    job, message_id = enqueue(broker)
    delivery = queue.next_delivery()
    assert queue.begin(*delivery)["attempts"] == 1
    assert queue.begin(*delivery)["attempts"] == 2
    assert queue.begin(*delivery) is None
    result = json.loads(broker.get("job:" + job["job_id"]))
    assert result["status"] == "failed"
    assert result["error"] == "attempts_exhausted"
    assert result["attempts"] == 2
    assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0


def test_real_atomic_collision_preserves_record_and_stream(broker):
    job, message_id = enqueue(broker)
    before = broker.get("job:" + job["job_id"]), broker.xrange(qh.STREAM)
    with pytest.raises(redis.ResponseError, match="job exists"):
        enqueue(broker)
    assert (broker.get("job:" + job["job_id"]), broker.xrange(qh.STREAM)) == before



def test_real_api_submit_worker_and_get_completed_job(broker, monkeypatch):
    from redis.asyncio import Redis as AsyncRedis
    def factory(*args, **kwargs):
        return AsyncRedis(unix_socket_path=broker.connection_pool.connection_kwargs["path"],
                          decode_responses=True, socket_timeout=1)
    monkeypatch.setattr(api.redis, "from_url", factory)
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: httpx.Response(200, json=[]))
    queue = qh.StreamQueue(broker, consumer="api-integration", poll_ms=1)
    with TestClient(api.app) as client:
        assert client.get("/readyz").status_code == 200
        response = client.post("/v2/exports", json={"repo": "team/repo", "format": "csv"})
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        assert broker.xlen(qh.STREAM) == 1
        store = UnitStore()
        worker.handle_delivery(queue, store, *queue.next_delivery(), public_client=store)
        result = client.get("/v2/exports/" + job_id).json()
        assert result["status"] == "done"
        assert result["worker"] == "api-integration"
        assert result["download_url"] == "https://download.invalid/" + result["object_name"]
        assert store.objects[result["object_name"]] == b"id,title,state,created_at\r\n"
        assert broker.xpending(qh.STREAM, qh.GROUP)["pending"] == 0


def test_real_api_startup_aborts_on_legacy_backlog(broker, monkeypatch):
    from redis.asyncio import Redis as AsyncRedis
    monkeypatch.setattr(api.redis, "from_url", lambda *a, **k: AsyncRedis(
        unix_socket_path=broker.connection_pool.connection_kwargs["path"], decode_responses=True))
    broker.lpush(qh.LEGACY_QUEUE, "job-abcdef12")
    before = broker.dump(qh.LEGACY_QUEUE)
    with pytest.raises(RuntimeError, match="export queue preflight failed"):
        with TestClient(api.app):
            pytest.fail("API must not start with legacy backlog")
    assert broker.dump(qh.LEGACY_QUEUE) == before
    assert not broker.exists(qh.STREAM)


def test_real_late_legacy_writer_blocks_ready_submit_and_poll(broker, monkeypatch):
    from redis.asyncio import Redis as AsyncRedis
    monkeypatch.setattr(api.redis, "from_url", lambda *a, **k: AsyncRedis(
        unix_socket_path=broker.connection_pool.connection_kwargs["path"], decode_responses=True))
    queue = qh.StreamQueue(broker, poll_ms=1)
    with TestClient(api.app) as client:
        broker.lpush(qh.LEGACY_PROCESSING, "job-abcdef12")
        before = set(broker.scan_iter()), broker.dump(qh.LEGACY_PROCESSING)
        assert client.get("/readyz").status_code == 503
        response = client.post("/v2/exports", json={"repo": "team/repo", "format": "csv"})
        assert response.status_code == 503
        assert response.json()["detail"] == "export queue unavailable"
        with pytest.raises(redis.ResponseError, match="legacy export backlog"):
            queue.next_delivery()
        assert (set(broker.scan_iter()), broker.dump(qh.LEGACY_PROCESSING)) == before
        assert broker.xlen(qh.STREAM) == 0
