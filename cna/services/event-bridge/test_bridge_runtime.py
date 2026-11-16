"""REAL app-component integration: private NATS 2.10 FILE storage, no fake JS.

Run: BROKER_NATS_SERVER=/root/arqweb-runtime-tools/nats-server \
 /tmp/arqweb-bridge-integration-venv/bin/python -m pytest -q -s test_bridge_runtime.py
No Gitea UI, full VM chaos, production service, or performance claims.
"""
import asyncio
import base64
from contextlib import asynccontextmanager
import importlib.util
import json
from pathlib import Path
import signal
import sys

import httpx
import nats
import pytest
from nats.js.api import AckPolicy, ConsumerConfig

from broker_fixtures import NatsBroker

import os
import app as app_module

pytestmark = pytest.mark.skipif(not os.environ.get("BROKER_NATS_SERVER"),
                                reason="private NATS fixture requires explicit opt-in")


def emit(event, **fields):
    print("BRIDGE_REAL_EVIDENCE " + json.dumps({"event": event, **fields}, default=str), flush=True)


@pytest.fixture
def bridge_loader(monkeypatch):
    # Current-main app/SDK; conftest replaces only network export with in-memory export.
    # Recreate broker clients/helper via actual application lifecycle, not module telemetry.
    monkeypatch.setattr(app_module, "WEBHOOK_SECRET", "private-integration-secret")
    monkeypatch.setenv("DLQ_READ_TOKEN", "synthetic-dlq-token")

    def load(url):
        monkeypatch.setattr(app_module, "NATS_URL", url)
        return app_module
    return load


@pytest.fixture(autouse=True)
def hard_test_deadline():
    # Independent wall-clock ceiling even if an awaited operation blocks.
    def expired(*_):
        raise TimeoutError("bridge integration exceeded hard 20-second test ceiling")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, 20)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@asynccontextmanager
async def private_broker():
    broker = NatsBroker()  # Unmodified fixture: watchdog 28s + cleanup, total <=30s.
    try:
        await asyncio.to_thread(broker.__enter__)
        assert broker.protocol_info["version"].startswith("2.10.")
        assert broker.tcp_listeners() == [("127.0.0.1", broker.port)]
        emit("broker_started", pid=broker.process.pid, url=broker.url,
             protocol_info=broker.protocol_info, config=broker.config_text)
        yield broker
    finally:
        if (broker.root / "broker.log").exists():
            emit("broker_log", text=(broker.root / "broker.log").read_text())
        await asyncio.to_thread(broker.close)
        assert broker.process is None or broker.process.poll() is not None
        emit("owned_broker_cleanup_verified", pid=broker.process.pid if broker.process else None,
             returncode=broker.process.returncode if broker.process else None,
             private_directory_removed=not broker.root.exists())


async def startup(bridge, observer):
    try:
        await bridge.app.router.startup()  # ASGITransport does NOT run lifespan.
    except Exception:
        for name in ("EVENTS", "BRIDGE_DLQ", "BRIDGE_ADVISORIES"):
            try:
                info = await observer.jetstream().stream_info(name)
                emit("startup_stream_diagnostic", name=name, config=info.config.as_dict())
            except Exception as exc:
                emit("startup_stream_diagnostic_error", name=name, error=repr(exc))
        try:
            info = await observer.jetstream().consumer_info("BRIDGE_ADVISORIES", "bridge-dlq-v1")
            emit("startup_consumer_diagnostic", config=info.config.as_dict())
        except Exception as exc:
            emit("startup_consumer_diagnostic_error", error=repr(exc))
        raise


async def until(probe, predicate, description, seconds=5):
    async with asyncio.timeout(seconds):
        while True:
            value = await probe()
            if predicate(value):
                return value
            await asyncio.sleep(0.025)


def test_real_webhook_requires_puback_and_stores_multi_token_issue(bridge_loader):
    async def run():
        async with asyncio.timeout(18), private_broker() as broker:
            observer = await nats.connect(broker.url, allow_reconnect=False, connect_timeout=1)
            bridge = bridge_loader(broker.url)
            try:
                await startup(bridge, observer)
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app),
                                            base_url="http://bridge") as client:
                    ready = await client.get("/readyz")
                    assert ready.status_code == 200, ready.text
                    events = [
                        ("push", "gitea.push", {"repository": {"full_name": "lab/repo"},
                         "ref": "refs/heads/test", "pusher": {"login": "qa"}, "commits": [{}]}),
                        ("issues", "gitea.issue.created", {"action": "opened",
                         "repository": {"full_name": "lab/repo"},
                         "issue": {"number": 7, "title": "multi-token"}, "sender": {"login": "qa"}}),
                    ]
                    for index, (event, subject, body) in enumerate(events, 1):
                        delivery = f"delivery-{index}"
                        response = await client.post("/webhook/private-integration-secret", json=body,
                            headers={"X-Gitea-Event": event, "X-Gitea-Delivery": delivery,
                                     "X-Request-ID": "request-webhook"})
                        assert response.status_code == 200, response.text
                        assert response.json() == {"status": "published", "event_id": delivery}
                        stored = await observer.jetstream().get_msg("EVENTS", seq=index)
                        envelope = json.loads(stored.data)
                        assert stored.subject == subject
                        assert envelope["event_id"] == delivery
                        assert envelope["request_id"] == "request-webhook"
                        assert envelope["type"] == subject
                        emit("webhook_persisted", seq=index, subject=stored.subject, envelope=envelope)
                    assert (await observer.jetstream().stream_info("EVENTS")).state.messages == 2
                    # Real broker rejection: remove the owning stream, not a fake PubAck.
                    await observer.jetstream().delete_stream("EVENTS")
                    rejected = await client.post("/webhook/private-integration-secret", json=events[0][2],
                        headers={"X-Gitea-Event": "push", "X-Gitea-Delivery": "must-not-succeed"})
                    assert rejected.status_code == 503, rejected.text
                    emit("webhook_real_no_stream_rejected", status=rejected.status_code, body=rejected.json())
            finally:
                await bridge.app.router.shutdown()
                await observer.close()
    asyncio.run(run())


def test_real_offline_advisory_restart_dedupe_and_ack_after_publish(bridge_loader):
    async def run():
        async with asyncio.timeout(18), private_broker() as broker:
            observer = await nats.connect(broker.url, allow_reconnect=False, connect_timeout=1)
            bridges = []
            try:
                first = bridge_loader(broker.url)
                bridges.append(first)
                await startup(first, observer)
                actual_configs = {}
                for stream in ("EVENTS", "BRIDGE_ADVISORIES", "BRIDGE_DLQ"):
                    config = (await observer.jetstream().stream_info(stream)).config
                    assert config.storage == "file" and config.duplicate_window == 120
                    actual_configs[stream] = config.as_dict()
                emit("actual_file_stream_contract", configs=actual_configs)
                await first.app.router.shutdown()
                assert first.nc.is_closed and not first.dlq.ready
                js = observer.jetstream()
                original = json.dumps({"event_id": "poison-event", "request_id": "poison-request",
                    "trace_context": {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"},
                    "payload": {"raw": "original café", "nested": [1, 2]}}, ensure_ascii=False).encode()
                source_ack = await js.publish("gitea.issue.created", original)
                poison = await js.pull_subscribe("gitea.issue.created", durable="poison-v2", stream="EVENTS",
                    config=ConsumerConfig(durable_name="poison-v2", ack_policy=AckPolicy.EXPLICIT,
                                          ack_wait=0.1, max_deliver=1))
                delivered = (await poison.fetch(1, timeout=1))[0]
                assert delivered.data == original
                # A second real pull makes the server evaluate max-deliver after ack_wait.
                await asyncio.sleep(0.15)
                try:
                    await poison.fetch(1, timeout=0.25)
                except (nats.errors.TimeoutError, asyncio.TimeoutError):
                    pass
                advisory_info = await until(lambda: js.stream_info("BRIDGE_ADVISORIES"),
                    lambda i: i.state.messages >= 1, "offline max-deliver advisory")
                raw_advisory = await js.get_msg("BRIDGE_ADVISORIES", seq=advisory_info.state.last_seq)
                advisory = json.loads(raw_advisory.data)
                assert advisory["stream"] == "EVENTS" and advisory["consumer"] == "poison-v2"
                assert advisory["stream_seq"] == source_ack.seq and advisory["deliveries"] == 1
                assert (await js.stream_info("BRIDGE_DLQ")).state.messages == 0
                emit("actual_maxdeliver_while_bridge_offline", advisory=advisory,
                     stored_advisories=advisory_info.state.messages, stored_dlq=0)

                wire = []
                async def published(msg):
                    wire.append("publish")
                async def acked(msg):
                    # Actual ACK wire message, then confirm broker FILE stream count.
                    state = (await js.stream_info("BRIDGE_DLQ")).state
                    wire.append("ack")
                    emit("actual_advisory_ack", subject=msg.subject, dlq_messages=state.messages,
                         wire_order=list(wire))
                await observer.subscribe("bridge.dlq.events", cb=published)
                await observer.subscribe("$JS.ACK.BRIDGE_ADVISORIES.>", cb=acked)
                await observer.flush()

                second = bridge_loader(broker.url)
                bridges.append(second)
                await startup(second, observer)
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=second.app),
                                            base_url="http://bridge") as client:
                    async def read_dlq():
                        response = await client.get("/dlq", headers={"X-DLQ-Token": "synthetic-dlq-token"})
                        if response.status_code != 200:
                            try:
                                await second.dlq.read()
                            except Exception as exc:
                                emit("dlq_read_diagnostic", status=response.status_code, error=repr(exc))
                            return {"total": -1, "http_status": response.status_code}
                        return response.json()
                    result = await until(read_dlq, lambda body: body["total"] == 1, "offline advisory DLQ")
                    entry = result["items"][0]
                    assert base64.b64decode(entry["original_payload_b64"]) == original
                    assert entry["event_id"] == "poison-event" and entry["request_id"] == "poison-request"
                    assert entry["trace_context"] == json.loads(original)["trace_context"]
                    assert entry["original_subject"] == "gitea.issue.created"
                    assert entry["advisory"]["id"] == advisory["id"]
                    stored = await js.get_msg("BRIDGE_DLQ", seq=1)
                    assert stored.headers["Nats-Msg-Id"] == f"dlq:EVENTS:poison-v2:{source_ack.seq}"
                    await until(lambda: asyncio.sleep(0, result=wire), lambda w: w.count("ack") >= 1,
                                "first actual ACK wire observation")
                    # New advisory ID, identical stream/consumer/seq: one stored DLQ item.
                    duplicate = {**advisory, "id": "different-advisory-id-same-source"}
                    duplicate_ack = await js.publish(raw_advisory.subject, json.dumps(duplicate).encode())
                    consumer = await until(lambda: js.consumer_info("BRIDGE_ADVISORIES", "bridge-dlq-v1"),
                        lambda c: c.ack_floor.stream_seq >= duplicate_ack.seq and c.num_ack_pending == 0,
                        "all advisory ACKs after DLQ PubAck")
                    result = await read_dlq()
                    assert result["total"] == 1
                    assert consumer.num_pending == 0 and consumer.delivered.consumer_seq >= 2
                    await until(lambda: asyncio.sleep(0, result=wire), lambda w: w.count("ack") >= 2,
                                "real ACK wire observation")
                    assert wire[:4] == ["publish", "ack", "publish", "ack"], wire
                    # Identity must include consumer AND sequence, not advisory ID alone.
                    next_source = await js.publish("gitea.issue.created", original)
                    for variant, expected_total in (({**advisory, "consumer": "poison-other"}, 2),
                                                     ({**advisory, "stream_seq": next_source.seq}, 3)):
                        published_advisory = await js.publish(raw_advisory.subject, json.dumps(variant).encode())
                        consumer = await until(lambda: js.consumer_info("BRIDGE_ADVISORIES", "bridge-dlq-v1"),
                            lambda c: c.ack_floor.stream_seq >= published_advisory.seq and c.num_ack_pending == 0,
                            "distinct deterministic identity ACK")
                        result = await read_dlq()
                        assert result["total"] == expected_total
                    identities = {(await js.get_msg("BRIDGE_DLQ", seq=i)).headers["Nats-Msg-Id"]
                                  for i in range(1, 4)}
                    assert identities == {f"dlq:EVENTS:poison-v2:{source_ack.seq}",
                                          f"dlq:EVENTS:poison-other:{source_ack.seq}",
                                          f"dlq:EVENTS:poison-v2:{next_source.seq}"}
                    emit("real_dedupe_and_ack_counts", dlq_messages=result["total"], identities=sorted(identities),
                         advisory_messages=(await js.stream_info("BRIDGE_ADVISORIES")).state.messages,
                         consumer=consumer.as_dict(), wire_order=wire)

                await second.app.router.shutdown()
                await observer.close()
                old_pid = broker.process.pid
                old_process = broker.process
                await asyncio.to_thread(broker.restart)  # Only this owned fixture child.
                # NATS 2.10 returns 1 for SIGTERM even after orderly shutdown.
                shutdown_log = (broker.root / "broker.log").read_text()
                assert old_process.poll() in (0, 1), "broker was killed rather than terminated"
                for marker in ("Initiating Shutdown...", "JetStream Shutdown", "Server Exiting.."):
                    assert f"[{old_pid}]" in shutdown_log and marker in shutdown_log
                emit("broker_graceful_restart", old_pid=old_pid, old_returncode=old_process.returncode,
                     new_pid=broker.process.pid, new_url=broker.url)
                observer = await nats.connect(broker.url, allow_reconnect=False, connect_timeout=1)
                js = observer.jetstream()
                third = bridge_loader(broker.url)
                bridges.append(third)
                await startup(third, observer)
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=third.app),
                                            base_url="http://bridge") as client:
                    assert (await client.get("/readyz")).status_code == 200
                    restarted = await client.get("/dlq", headers={"X-DLQ-Token": "synthetic-dlq-token"})
                    assert restarted.status_code == 200, restarted.text
                    assert restarted.json() == result
                    assert (await js.stream_info("BRIDGE_DLQ")).state.messages == 3
                    # Repeat the same identity after broker + app restart, still within 120s.
                    replay = await js.publish(raw_advisory.subject,
                        json.dumps({**advisory, "id": "post-restart-new-advisory-id"}).encode())
                    final_consumer = await until(lambda: js.consumer_info("BRIDGE_ADVISORIES", "bridge-dlq-v1"),
                        lambda c: c.ack_floor.stream_seq >= replay.seq and c.num_ack_pending == 0,
                        "post-restart deduplicated advisory ACK")
                    assert (await client.get("/dlq", headers={"X-DLQ-Token": "synthetic-dlq-token"})).json() == result
                    emit("dlq_survives_graceful_broker_and_app_restart", body=restarted.json(),
                         consumer=final_consumer.as_dict())
            finally:
                for bridge in reversed(bridges):
                    await bridge.app.router.shutdown()
                await observer.close()
    asyncio.run(run())


def test_real_empty_dlq_is_readable_without_requesting_sequence_zero(bridge_loader):
    """Empty broker state must be independently readable, without retry masking."""
    async def run():
        async with asyncio.timeout(18), private_broker() as broker:
            observer = await nats.connect(broker.url, allow_reconnect=False, connect_timeout=1)
            bridge = bridge_loader(broker.url)
            try:
                await startup(bridge, observer)
                info = await observer.jetstream().stream_info("BRIDGE_DLQ")
                assert info.state.messages == 0
                try:
                    await bridge.dlq.read()
                except Exception as exc:
                    emit("empty_dlq_exact_repro", first_seq=info.state.first_seq, last_seq=info.state.last_seq,
                         error_type=type(exc).__name__, error=str(exc),
                         broker_error_code=getattr(exc, "err_code", None),
                         broker_description=getattr(exc, "description", None))
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app),
                                            base_url="http://bridge") as client:
                    response = await client.get("/dlq", headers={"X-DLQ-Token": "synthetic-dlq-token"})
                    assert response.status_code == 200, response.text
                    assert response.json() == {"total": 0, "items": [], "truncated": False}
            finally:
                await bridge.app.router.shutdown()
                await observer.close()
    asyncio.run(run())
