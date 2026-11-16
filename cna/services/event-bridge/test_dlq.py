"""UNIT: in-process broker fakes; not real NATS/chaos evidence."""
import asyncio
import base64
import importlib
import importlib.util
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nats.js.errors import NotFoundError


def helper():
    assert importlib.util.find_spec("newdlq"), "durable DLQ helper is missing"
    return importlib.import_module("newdlq")


class UnitBroker:
    """UNIT fake broker state deliberately survives helper recreation."""
    def __init__(self):
        self.original = SimpleNamespace(subject="gitea.push", data=json.dumps({
            "event_id": "event-1", "request_id": "request-1",
            "trace_context": {"traceparent": "00-context"}, "payload": {"body": "original"}
        }).encode())
        self.records = []
        self.ids = set()
        self.order = []
        self.fail_publish = False

    async def get_msg(self, stream, seq):
        self.order.append("fetch")
        assert stream == "EVENTS" and seq == 42
        return self.original

    async def publish(self, subject, data, headers=None, **kwargs):
        if self.fail_publish:
            raise TimeoutError("nats://user:private@broker")
        msg_id = (headers or {}).get("Nats-Msg-Id")
        duplicate = msg_id in self.ids
        if not duplicate:
            self.records.append((subject, json.loads(data)))
            self.ids.add(msg_id)
        self.order.append("puback")
        return SimpleNamespace(stream="BRIDGE_DLQ", seq=len(self.records), duplicate=duplicate)


def advisory(broker, **changes):
    body = {"id": "advisory-1", "stream": "EVENTS", "consumer": "Audit", "stream_seq": 42, "deliveries": 3}
    body.update(changes)
    async def ack():
        broker.order.append("ack")
    return SimpleNamespace(data=json.dumps(body).encode(), ack=AsyncMock(side_effect=ack))


def test_unit_original_payload_is_persisted_before_advisory_ack():
    async def run():
        broker = UnitBroker()
        msg = advisory(broker)
        dlq = helper().DurableDLQ(broker)
        await dlq.handle(msg)
        assert broker.order == ["fetch", "puback", "ack"]
        entry = broker.records[0][1]
        assert entry["original_subject"] == "gitea.push"
        assert base64.b64decode(entry["original_payload_b64"]) == broker.original.data
        assert entry["event_id"] == "event-1"
        assert entry["request_id"] == "request-1"
        assert entry["trace_context"] == {"traceparent": "00-context"}
        assert entry["advisory"]["stream_seq"] == 42
    asyncio.run(run())


@pytest.mark.parametrize("failure", ["publish", "fetch", "missing"])
def test_unit_broker_failure_is_observable_unacked_then_retry_succeeds(failure, caplog):
    async def run():
        broker = UnitBroker()
        dlq = helper().DurableDLQ(broker)
        msg = advisory(broker)
        original_fetch = broker.get_msg
        if failure == "publish":
            broker.fail_publish = True
        else:
            error = NotFoundError if failure == "missing" else TimeoutError
            broker.get_msg = AsyncMock(side_effect=error())
        await dlq.handle(msg)
        msg.ack.assert_not_awaited()
        assert not broker.records
        assert "dlq_retry_required" in caplog.text
        assert "private" not in caplog.text
        broker.fail_publish = False
        broker.get_msg = original_fetch
        await dlq.handle(msg)
        msg.ack.assert_awaited_once()
        assert len(broker.records) == 1
    asyncio.run(run())


def test_unit_duplicate_advisories_dedupe_at_broker_across_helper_restart():
    async def run():
        broker = UnitBroker()
        for identifier in ("first-advisory", "repeated-max-delivery"):
            msg = advisory(broker, id=identifier)
            await helper().DurableDLQ(broker).handle(msg)
            msg.ack.assert_awaited_once()
        assert len(broker.records) == 1
        assert broker.ids == {"dlq:EVENTS:Audit:42"}
    asyncio.run(run())


@pytest.mark.parametrize("stream", ["BRIDGE_DLQ", "BRIDGE_ADVISORIES", "OTHER"])
def test_unit_non_source_advisories_never_recurse(stream):
    async def run():
        broker = UnitBroker()
        msg = advisory(broker, stream=stream)
        await helper().DurableDLQ(broker).handle(msg)
        assert broker.order == ["ack"]
        assert not broker.records
    asyncio.run(run())


class UnitResources(UnitBroker):
    def __init__(self):
        super().__init__()
        self._nc = SimpleNamespace(max_payload=1024 * 1024)
        self.streams = {}
        self.consumer = None
        self.subscription = SimpleNamespace(unsubscribe=AsyncMock(), _closed=False)
        self.subscribe_options = None

    async def stream_info(self, name):
        if name not in self.streams:
            raise NotFoundError()
        return SimpleNamespace(config=self.streams[name], state=SimpleNamespace(
            messages=len(self.records), first_seq=1, last_seq=len(self.records)))

    async def add_stream(self, config):
        self.streams[config.name] = config
        return await self.stream_info(config.name)

    async def consumer_info(self, stream, name):
        if self.consumer is None:
            raise NotFoundError()
        return SimpleNamespace(config=self.consumer)

    async def subscribe(self, subject, **kwargs):
        self.consumer = kwargs["config"]
        self.subscribe_options = {"subject": subject, **kwargs}
        return self.subscription


def test_unit_setup_is_file_backed_durable_manual_ack_with_replay():
    async def run():
        from nats.js.api import StorageType, AckPolicy, DeliverPolicy
        broker = UnitResources()
        dlq = helper().DurableDLQ(broker)
        await dlq.setup()
        assert dlq.ready
        assert all(c.storage == StorageType.FILE for c in broker.streams.values())
        assert broker.streams["BRIDGE_DLQ"].duplicate_window == 120
        assert broker.streams["BRIDGE_ADVISORIES"].subjects == [helper().ADVISORY_SUBJECT]
        options = broker.subscribe_options
        assert options["manual_ack"] is True
        assert options["stream"] == "BRIDGE_ADVISORIES"
        assert options["durable"] == "bridge-dlq-v1"
        assert options["cb"] == dlq.handle
        assert options["config"].ack_policy == AckPolicy.EXPLICIT
        assert options["config"].deliver_policy == DeliverPolicy.ALL
        assert options["config"].max_deliver == -1
        assert options["config"].mem_storage is False
    asyncio.run(run())


@pytest.mark.parametrize("resource", ["EVENTS", "BRIDGE_DLQ", "BRIDGE_ADVISORIES", "consumer"])
def test_unit_resource_config_conflicts_never_claim_ready(resource):
    async def run():
        from nats.js.api import StorageType, AckPolicy
        broker = UnitResources()
        first = helper().DurableDLQ(broker)
        await first.setup()
        if resource == "consumer":
            broker.consumer.ack_policy = AckPolicy.NONE
        else:
            broker.streams[resource].storage = StorageType.MEMORY
        restarted = helper().DurableDLQ(broker)
        with pytest.raises(helper().ResourceConflict):
            await restarted.setup()
        assert restarted.ready is False
    asyncio.run(run())


def test_unit_restarted_bridge_reads_same_bounded_broker_state():
    async def run():
        broker = UnitResources()
        await helper().DurableDLQ(broker).setup()
        await helper().DurableDLQ(broker).handle(advisory(broker))
        for n in range(5):
            broker.records.append(("bridge.dlq.events", {"n": n}))
        reads = []
        async def get_msg(stream, seq):
            reads.append(seq)
            assert stream == "BRIDGE_DLQ"
            return SimpleNamespace(data=json.dumps(broker.records[seq-1][1]).encode())
        broker.get_msg = get_msg
        restarted = helper().DurableDLQ(broker)
        result = await restarted.read(limit=2)
        assert result["total"] == 6
        assert result["items"] == [{"n": 4}, {"n": 3}]
        assert reads == [6, 5]
        assert result["truncated"] is True
        broker.stream_info = AsyncMock(side_effect=TimeoutError())
        with pytest.raises(TimeoutError):
            await restarted.read(limit=2)
    asyncio.run(run())


def test_unit_fresh_empty_dlq_never_requests_sequence_zero():
    async def run():
        broker = UnitResources()
        broker.stream_info = AsyncMock(return_value=SimpleNamespace(
            state=SimpleNamespace(messages=0, first_seq=0, last_seq=0)))
        broker.get_msg = AsyncMock(side_effect=ValueError("invalid sequence zero"))
        result = await helper().DurableDLQ(broker).read()
        assert result == {"total": 0, "items": [], "truncated": False}
        broker.get_msg.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("deleted", [False, True])
def test_unit_sparse_dlq_reads_remain_bounded_despite_snapshot_races(deleted):
    async def run():
        broker = UnitResources()
        broker.stream_info = AsyncMock(return_value=SimpleNamespace(
            state=SimpleNamespace(messages=2, first_seq=1, last_seq=10000)))
        async def get_msg(stream, seq):
            if deleted or seq != 10000:
                raise NotFoundError()
            return SimpleNamespace(data=b'{"n": 10000}')
        broker.get_msg = AsyncMock(side_effect=get_msg)
        result = await helper().DurableDLQ(broker).read(limit=2)
        assert result == {"total": 2, "items": [] if deleted else [{"n": 10000}], "truncated": True}
        assert [call.kwargs["seq"] for call in broker.get_msg.await_args_list] == [10000, 9999]
    asyncio.run(run())


def test_unit_live_resource_check_detects_deleted_stream():
    async def run():
        broker = UnitResources()
        dlq = helper().DurableDLQ(broker)
        await dlq.setup()
        await dlq.check_resources()
        del broker.streams["BRIDGE_ADVISORIES"]
        with pytest.raises(NotFoundError):
            await dlq.check_resources()
    asyncio.run(run())


@pytest.mark.parametrize("puback", [None, SimpleNamespace(stream="OTHER", seq=1)])
def test_unit_dlq_does_not_ack_without_expected_puback(puback, caplog):
    async def run():
        broker = UnitBroker()
        broker.publish = AsyncMock(return_value=puback)
        msg = advisory(broker)
        await helper().DurableDLQ(broker).handle(msg)
        msg.ack.assert_not_awaited()
        assert "dlq_retry_required" in caplog.text
    asyncio.run(run())


class UnreadableLiveness:
    @property
    def _closed(self):
        raise RuntimeError("unreadable liveness")


@pytest.mark.parametrize("layer", ["wrapper", "underlying"])
@pytest.mark.parametrize("state", [None, 0, "", [], "missing", "raises", True, 1, "false"],
                         ids=["none", "zero", "empty-string", "empty-list", "missing", "raises",
                              "closed", "one", "nonempty-string"])
def test_unit_liveness_unknown_fails_before_broker_reads(layer, state):
    async def run():
        broker = UnitResources()
        dlq = helper().DurableDLQ(broker)
        await dlq.setup()
        if state == "missing":
            candidate = SimpleNamespace()
        elif state == "raises":
            candidate = UnreadableLiveness()
        else:
            candidate = SimpleNamespace(_closed=state)
        dlq.subscription = candidate if layer == "wrapper" else SimpleNamespace(_closed=False, _sub=candidate)
        broker.stream_info = AsyncMock(wraps=broker.stream_info)
        broker.consumer_info = AsyncMock(wraps=broker.consumer_info)
        expected_error = RuntimeError if state == "raises" else helper().ResourceConflict
        with pytest.raises(expected_error):
            await dlq.check_resources()
        broker.stream_info.assert_not_awaited()
        broker.consumer_info.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("wrapped", [False, True])
def test_unit_liveness_known_false_checks_broker_contract(wrapped):
    async def run():
        broker = UnitResources()
        dlq = helper().DurableDLQ(broker)
        await dlq.setup()
        live = SimpleNamespace(_closed=False)
        dlq.subscription = SimpleNamespace(_closed=False, _sub=live) if wrapped else live
        broker.stream_info = AsyncMock(wraps=broker.stream_info)
        broker.consumer_info = AsyncMock(wraps=broker.consumer_info)
        await dlq.check_resources()
        assert broker.stream_info.await_count == 3
        broker.consumer_info.assert_awaited_once_with("BRIDGE_ADVISORIES", "bridge-dlq-v1")
    asyncio.run(run())
