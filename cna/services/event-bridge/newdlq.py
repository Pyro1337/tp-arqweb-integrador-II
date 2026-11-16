"""JetStream-backed DLQ. Delivery is at least once, never exactly once."""
import asyncio
import base64
import nats
import json
import logging

log = logging.getLogger("event-bridge.dlq")
from datetime import datetime, timezone
from nats.js.api import StreamConfig, StorageType, RetentionPolicy, DiscardPolicy, ConsumerConfig, AckPolicy, DeliverPolicy
from nats.js.errors import NotFoundError

ADVISORY_SUBJECT = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
DURABLE = "bridge-dlq-v1"

# Bounded FILE storage: 16 + 8 + 4 = 28 MiB, leaving 4 MiB in the
# private 32 MiB broker. Existing incompatible streams require operator action;
# setup validates, never updates streams or deletes retained messages.
EVENTS_MAX_BYTES = 16 * 1024**2
DLQ_MAX_BYTES = 8 * 1024**2
ADVISORIES_MAX_BYTES = 4 * 1024**2

# Single-node/client-publisher bounded profile. See README.md for the full proof.
SOURCE_MAX_MSG_BYTES = 16 * 1024
ADVISORY_MAX_MSG_BYTES = 4 * 1024
SUBJECT_MAX_BYTES = 4096
# Compact ensure_ascii JSON expands each input byte by at most six. Selected
# source metadata are disjoint subtrees; the exact source is additionally base64.
DLQ_JSON_MAX_BYTES = (4 * ((SOURCE_MAX_MSG_BYTES + 2) // 3)
                      + 6 * SOURCE_MAX_MSG_BYTES + 6 * ADVISORY_MAX_MSG_BYTES
                      + 6 * SUBJECT_MAX_BYTES + 4096)
DLQ_HEADER_MAX_BYTES = 6 * ADVISORY_MAX_MSG_BYTES + 256
DLQ_MAX_MSG_BYTES = DLQ_JSON_MAX_BYTES + DLQ_HEADER_MAX_BYTES
# Also budget the JS get-message API's base64 wrapper around a DLQ record.
BROKER_MIN_MAX_PAYLOAD = 512 * 1024
assert 4 * ((DLQ_MAX_MSG_BYTES + 2) // 3) + 6 * SUBJECT_MAX_BYTES + 4096 < BROKER_MIN_MAX_PAYLOAD


def validate_payload_budget(nc):
    if type(nc.max_payload) is not int or nc.max_payload < BROKER_MIN_MAX_PAYLOAD:
        raise ResourceConflict("incompatible broker payload budget")


class _SubjectBudgetProbe(nats.aio.client.Client):
    # nats-py 2.9.0 may overwrite last_error with EOF after a protocol error.
    # Capture the parsed signal before delegating; a disconnect alone proves nothing.
    def __init__(self):
        super().__init__()
        self.control_line_rejected = False
        self.protocol_error_received = asyncio.Event()

    async def _process_err(self, err_msg):
        if err_msg.strip().strip("'").lower() == "maximum control line exceeded":
            self.control_line_rejected = True
        self.protocol_error_received.set()
        await super()._process_err(err_msg)


async def validate_transport(nc):
    """Verify the connected server enforces the subject bound (INFO omits MCL).

    A disposable Core SUB with 4097 argument bytes must be rejected for the
    control-line limit, before ACL lookup. No publish or JetStream mutation.
    Never send the deliberately invalid line on the bridge's actual connection.
    """
    validate_payload_budget(nc)
    probe = None
    async def quiet_error(_):
        pass  # Only synthetic protocol data; don't log server exception text.
    try:
        async with asyncio.timeout(2):
            probe = _SubjectBudgetProbe()
            await probe.connect(nc.connected_url.geturl(), allow_reconnect=False,
                                connect_timeout=1, error_cb=quiet_error)
            subject = "_bridge_size_probe." + "x" * (SUBJECT_MAX_BYTES - 1 - len("_bridge_size_probe."))
            await probe.subscribe(subject)
            try:
                await probe.flush(timeout=1)
            except (nats.errors.Error, asyncio.TimeoutError):
                pass
            # A flush PONG may precede the oversized SUB's parsed -ERR. Wait for
            # that dedicated signal, not a close/EOF or an arbitrary flush error.
            try:
                await asyncio.wait_for(probe.protocol_error_received.wait(), timeout=1)
            except asyncio.TimeoutError:
                raise ResourceConflict("incompatible broker subject budget") from None
            if not probe.control_line_rejected:
                raise ResourceConflict("incompatible broker subject budget")
    finally:
        if probe is not None:
            await probe.close()


class ResourceConflict(RuntimeError):
    """Broker configuration differs from the required durable contract."""


def _safe_metadata(entry):
    """Keep the record and exact source; explicitly fall back only its projection."""
    metadata = {key: entry.get(key) for key in ("event_id", "request_id", "trace_context")}
    try:
        # ASCII escaping alone hides lone surrogates, and default JSON permits
        # NaN/Infinity (including overflow parsed from valid JSON such as 1e999).
        json.dumps(metadata, ensure_ascii=False, allow_nan=False).encode("utf-8", errors="strict")
    except (ValueError, UnicodeEncodeError, RecursionError):
        entry.update(event_id=None, request_id=None, trace_context={}, metadata_unavailable=True)
    return entry


class DurableDLQ:
    def __init__(self, js):
        self.js = js
        self.ready = False
        self.subscription = None

    @staticmethod
    def stream_configs():
        return [StreamConfig(
            name=name, subjects=subjects, storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS, max_msgs=-1, max_bytes=max_bytes,
            max_age=0, duplicate_window=120,
            max_msgs_per_subject=-1, max_msg_size=max_msg_size, discard=DiscardPolicy.OLD,
        ) for name, subjects, max_bytes, max_msg_size in (
            ("EVENTS", ["gitea.>", "export.>", "media.>"], EVENTS_MAX_BYTES, SOURCE_MAX_MSG_BYTES),
            ("BRIDGE_DLQ", ["bridge.dlq.events"], DLQ_MAX_BYTES, DLQ_MAX_MSG_BYTES),
            ("BRIDGE_ADVISORIES", [ADVISORY_SUBJECT], ADVISORIES_MAX_BYTES, ADVISORY_MAX_MSG_BYTES),
        )]

    @staticmethod
    def validate_stream(actual, expected):
        fields = ("storage", "retention", "max_msgs", "max_bytes", "max_age", "no_ack", "sealed", "duplicate_window",
                  "discard", "max_msg_size")
        if sorted(actual.subjects or []) != sorted(expected.subjects) or any(
            getattr(actual, field) != getattr(expected, field) for field in fields
        ) or any(
            # Omitted SDK defaults and broker -1 both mean unlimited.
            (-1 if getattr(actual, field) is None else getattr(actual, field)) != -1
            for field in ("max_msgs_per_subject",)
        ) or any(getattr(actual, field, None) for field in (
            "subject_transform", "mirror", "sources", "republish"
        )):
            # Rewriting, importing or republishing may break exact stored-subject
            # and body semantics. Even apparently identity transforms are not owned.
            raise ResourceConflict("incompatible stream configuration")

    @staticmethod
    def consumer_config():
        return ConsumerConfig(
            durable_name=DURABLE, ack_policy=AckPolicy.EXPLICIT,
            deliver_policy=DeliverPolicy.ALL, ack_wait=30, max_deliver=-1,
            filter_subject=ADVISORY_SUBJECT, mem_storage=False, headers_only=False,
        )

    @classmethod
    def validate_consumer(cls, actual):
        expected = cls.consumer_config()
        if any(getattr(actual, field) != getattr(expected, field) for field in (
            "durable_name", "ack_policy", "deliver_policy", "ack_wait", "max_deliver", "filter_subject"
        )) or actual.headers_only is True or actual.mem_storage is True or actual.filter_subjects or actual.inactive_threshold:
            raise ResourceConflict("incompatible advisory consumer configuration")

    async def setup(self):
        self.ready = False
        validate_payload_budget(self.js._nc)
        # Preflight ALL existing resources before any create or subscribe. A late
        # conflict must not leave partially provisioned resources behind.
        missing = []
        for config in self.stream_configs():
            try:
                info = await self.js.stream_info(config.name)
            except NotFoundError:
                missing.append(config)
            else:
                self.validate_stream(info.config, config)
        try:
            info = await self.js.consumer_info("BRIDGE_ADVISORIES", DURABLE)
        except NotFoundError:
            pass
        else:
            self.validate_consumer(info.config)
        for config in missing:
            self.validate_stream((await self.js.add_stream(config=config)).config, config)
        self.subscription = await self.js.subscribe(
            ADVISORY_SUBJECT, stream="BRIDGE_ADVISORIES", durable=DURABLE,
            config=self.consumer_config(), cb=self.handle, manual_ack=True,
        )
        self.ready = True

    async def check_resources(self):
        # nats-py PushSubscription/Subscription.unsubscribe sets _closed and
        # removes local interest; check both wrapper and underlying subscription.
        # This SDK has no public is_active property. Unknown state fails closed.
        sub = self.subscription
        if (not self.ready or sub is None or getattr(sub, "_closed", True) is not False
                or getattr(getattr(sub, "_sub", sub), "_closed", True) is not False):
            raise ResourceConflict("advisory subscription unavailable")
        validate_payload_budget(self.js._nc)
        for config in self.stream_configs():
            self.validate_stream((await self.js.stream_info(config.name)).config, config)
        self.validate_consumer((await self.js.consumer_info("BRIDGE_ADVISORIES", DURABLE)).config)

    async def read(self, limit=50):
        limit = max(1, min(int(limit), 100))
        state = (await self.js.stream_info("BRIDGE_DLQ")).state
        if state.messages == 0:
            # Fresh JetStream streams have first_seq == last_seq == 0 (not fetchable).
            return {"total": 0, "items": [], "truncated": False}
        items = []
        # Bound broker requests even if deletions leave sparse sequence numbers.
        lower = max(state.first_seq, state.last_seq - limit + 1)
        for seq in range(state.last_seq, lower - 1, -1):
            try:
                message = await self.js.get_msg("BRIDGE_DLQ", seq=seq)
            except NotFoundError:
                continue  # Snapshot raced an operator deletion; not a fetch failure.
            # Old writers may have persisted unsafe metadata. Adapt the view,
            # never rewrite broker bytes or discard the offending record/list.
            items.append(_safe_metadata(json.loads(message.data)))
        return {"total": state.messages, "items": items, "truncated": state.messages > len(items)}

    async def handle(self, msg):
        try:
            await self._persist(msg)
        except Exception:
            # Never interpolate exception text, broker URLs, or payloads.
            log.warning("dlq_retry_required: original unavailable or broker operation failed; advisory unacked")

    async def _persist(self, msg):
        advisory = json.loads(msg.data)
        if advisory.get("stream") != "EVENTS":
            await msg.ack()
            return
        seq = advisory["stream_seq"]
        consumer = advisory["consumer"]
        if not isinstance(seq, int) or isinstance(seq, bool) or seq <= 0 or not isinstance(consumer, str) or not consumer:
            raise ValueError("invalid source advisory")
        original = await self.js.get_msg("EVENTS", seq=seq)
        # The SDK represents an existing zero-byte record as data=None. Access
        # the attribute first: absent records/attributes must still fail unacked.
        payload = original.data
        if payload is None:
            payload = b""
        try:
            envelope = json.loads(payload)
        except (ValueError, UnicodeDecodeError, RecursionError):
            envelope = {}
        if not isinstance(envelope, dict):
            envelope = {}
        entry = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "advisory": advisory,
            "original_subject": original.subject,
            "original_payload_b64": base64.b64encode(payload).decode("ascii"),
            "event_id": envelope.get("event_id"),
            "request_id": envelope.get("request_id"),
            "trace_context": envelope.get("trace_context", {}),
        }
        _safe_metadata(entry)
        encoded = json.dumps(entry, separators=(",", ":"), allow_nan=False).encode()
        ack = await self.js.publish(
            "bridge.dlq.events", encoded, timeout=2,
            headers={"Nats-Msg-Id": f"dlq:EVENTS:{consumer}:{seq}"},
        )
        if ack is None or ack.stream != "BRIDGE_DLQ" or type(ack.seq) is not int or ack.seq <= 0:
            raise RuntimeError("DLQ PubAck unavailable")
        await msg.ack()
