# Event bridge: bounded durable payload contract

This is the service-owned contract for the single-node, client-publisher profile
exercised with **NATS 2.10.24 / nats-py 2.9.0**. It is component evidence, **not a
VM, full-stack, production, power-loss, or performance proof**.

## Required bounds

`newdlq.py` is the executable authority; startup creates only absent resources.

| Resource | Subjects | FILE retention cap | Per-message cap |
| --- | --- | --- | --- |
| `EVENTS` | `gitea.>`, `export.>`, `media.>` | 16 MiB | 16 KiB (16,384 bytes) |
| `BRIDGE_ADVISORIES` | `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>` | 4 MiB | 4 KiB (4,096 bytes) |
| `BRIDGE_DLQ` | `bridge.dlq.events` | 8 MiB | 198,232 bytes (derived below) |

All streams use limits retention, discard-old, unlimited message count/age and
per-subject count, and a 120-second duplicate window. The aggregate FILE cap is
28 MiB; the private test broker remains bounded at **16 MiB memory / 64 MiB FILE**.
Those store budgets are not process-RSS or filesystem quotas.

On this broker, `max_msg_size` counts **body plus serialized NATS headers**, not
subject length. The HTTP bridge checks the final serialized envelope (not the
incoming HTTP body or title alone) against 16,384 bytes **before publishing**.
An over-limit envelope returns fixed HTTP 413 `event envelope too large` and is
not published. Other client producers are constrained by the EVENTS stream cap;
headers consume the same allowance. This is not an HTTP request-body memory cap.

The connected broker must advertise integer **`max_payload >= 524288` (512 KiB)**.
INFO does not advertise `max_control_line`. Startup and readiness therefore use a
separate, disposable Core connection to the connected server, with reconnect
disabled. A synthetic oversized SUB has 4,097 argument bytes; it must produce the
**parsed, exact `Maximum Control Line Exceeded` protocol error**. This establishes
the conservative **4,096-byte subject budget** used below; it is not a promise
that every subject of that length can be published (PUB/SUB/reply arguments also
consume control-line space). The intended server control-line bound is at most
4,096 bytes. An 8,192-byte permissive profile fails closed.

A flush PONG is not proof that the invalid SUB was rejected: the probe waits up
to one additional second for a dedicated parsed-protocol-error event, within the
existing two-second validation deadline. A disconnect, EOF, network exception,
unrelated protocol error, or silence never validates the bound. The probe is
closed in `finally`; it performs **no publish or JetStream mutation** and never
sends the invalid line on the service's actual connection. Unsupported server,
authentication, or connection behavior fails closed rather than bypassing the
check. This is not a redesigned authenticated/TLS/cluster discovery mechanism.

## Encoding and transport budget

For source body bound `S=16384`, advisory JSON bound `A=4096` and conservative
original subject bound `T=4096`, compact ensure-ASCII JSON reserves:

```
DLQ_JSON_MAX_BYTES = 4*ceil(S/3) + 6*S + 6*A + 6*T + 4096 = 173400
DLQ_HEADER_MAX_BYTES = 6*A + 256                         = 24832
DLQ_MAX_MSG_BYTES = DLQ_JSON_MAX_BYTES + DLQ_HEADER_MAX_BYTES = 198232
JS get-message wrapper reserve = 4*ceil(198232/3) + 6*T + 4096 = 292984
292984 < 524288
```

The source is retained exactly as base64. For an existing fetched record,
nats-py 2.9.0 represents a zero-byte body as `RawStreamMsg.data=None`; the helper
normalizes that representation once to `b''` for JSON extraction and base64
(the base64 string is empty). A missing fetch, absent record or absent data
attribute is not an empty source and remains unacked. The advisory still requires
a valid DLQ PubAck before ACK, including for headers-only empty bodies.
Selected event/request/trace metadata
are disjoint subtrees of the bounded source and are additionally projected into
the DLQ entry. JSON escaping can expand each input byte by at most six; advisory
and original-subject escaping and the dedupe header are budgeted separately.
The fixed reserves cover record fields, timestamp, framing and wrapper metadata.
For undecodable or too-deep source metadata, the exact original bytes still
survive; metadata extraction is not guaranteed for arbitrary binary/nesting.
Projected `event_id`, `request_id` and `trace_context` are checked together for
strict JSON (no NaN/Infinity, including numeric overflow) and Unicode-scalar
UTF-8 (no lone surrogates). Unsafe or unserializably deep projections explicitly
fall back to `event_id: null`, `request_id: null`, `trace_context: {}` and
`metadata_unavailable: true`. The original base64, subject and advisory remain
intact. This check applies before new publication and when inspecting records
persisted by older writers; read-time fallback changes only the returned view,
not stored broker bytes. No offending item or whole list is silently dropped.
This is a selected-metadata fallback, not recovery of arbitrary malformed DLQ
records or unsafe fields outside those projections. Inspection authentication
and the valid-PubAck-before-advisory-ACK gate are unchanged.
Tests cover exact-cap binary bytes, worst escaping, deep metadata, max-cap
advisories and a real max-delivery advisory from an accepted exact-cap HTTP event.
This domain assumes the owned single-node stream configuration: subject
transforms, mirror/source imports and republish are rejected, not auto-adopted.

## Existing resources: reject, do not migrate

An existing unlimited or differently capped legacy stream is **incompatible**,
even when empty. Startup preflights all existing streams and the advisory durable
before creating missing streams or binding a subscriber. Conflicts fail startup;
readiness returns fixed HTTP 503. No implicit stream update, purge, deletion,
subject rewrite or data conversion occurs. Preservation regressions assert
unchanged resource configuration/state and exact retained bytes after rejection.
Operators must reconcile retained data and coordinate producers/consumers before
an explicitly reviewed migration; restarting the bridge does not migrate it.

The durable `bridge-dlq-v1` uses explicit manual ACK, deliver-all, 30-second ACK
wait and unlimited max-deliver. A valid expected-stream PubAck with an **exact
positive integer** sequence is required for ingress and DLQ publication (booleans
are rejected). The advisory is ACKed only after DLQ PubAck; failures stay unacked.
Deterministic source-identity dedupe is bounded by the 120-second window, not
exactly-once delivery. Shutdown preserves streams and durable consumers.

DLQ inspection still requires explicitly configured `DLQ_READ_TOKEN` and
`X-DLQ-Token`; base64 is not redaction. Missing inspection credentials disable
inspection, not background processing. Security, metric names, trace-carrier and
production gRPC exporter contracts are not changed by this repair. Tests replace
only network export with an in-memory exporter; no live collector proof is made.

## Parent-owned deployment boundary (read-only inspection)

The inspected `cna/deploy/docker-compose.yml`, `services.nats`, starts
`nats:2.10-alpine` with `-js -sd /data -m 8222`; it does not explicitly override
payload/control-line limits or bootstrap these streams. Repository searches found
no separate stream bootstrap implementation. The current helper self-provisions
absent resources, so **no missing parent bootstrap is claimed and no mandatory
parent source edit was identified for a fresh compatible broker**.

The compose image tag is not pinned to the tested patch, and its implicit limits
are not a deployment proof. If the parent elects to pin this profile, the exact
owner path is `cna/deploy/docker-compose.yml`, `services.nats` (or a configuration
file mounted there): require payload >=512 KiB, control-line <=4,096, and FILE
capacity above the 28-MiB aggregate with margin. A live incompatible broker or
legacy stream requires explicit operator action; the bridge must not auto-update
it. No parent deploy, docs or bootstrap files were changed here.

## Local evidence and scope

Use the explicitly owned executable and isolated interpreter:

```
BROKER_NATS_SERVER=/root/arqweb-runtime-tools/nats-server \
PYTHONDONTWRITEBYTECODE=1 \
/tmp/arqweb-bridge-integration-venv/bin/python -m pytest -p no:cacheprovider -q -s
```

Fixtures use only owned ephemeral loopback listeners, private temporary stores,
bounded startup/scenario deadlines and the existing lifetime watchdog/cleanup.
Graceful broker restart proves the exercised FILE persistence only, not VM loss,
SIGKILL, disk failure, multi-node routing or production availability. Repeat runs
are stability executions of the same identities, not additional unique tests.
The authoritative fix report is outside this source tree at
`/root/arqweb-integration-evidence/bridge-fix1.json`; historical `bridge-fix1.*`
logs remain immutable, and completion logs use `bridge-fix1-completion.*`.
Independent review and parent publication approval remain separate gates.
For repair cycle 2, the parent observed four metadata HTTP failures (500 instead
of 200), covering generated/historical surrogate/nonfinite records, in
`/root/arqweb-integration-evidence/bridge-fix2-foreground-metadata-red.xml`
and its matching log. The subsequent metadata fallback implementation has not
been executed by its implementer; no post-repair GREEN is claimed here. The
implementation handoff is `bridge-fix2-phaseB.json` in that evidence directory;
parent execution and independent review are still required.
