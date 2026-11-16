# Export queue recovery — current-main integration

Local protocol implementation, **not production / VM-chaos / full-stack acceptance**.
Historical team evidence in `cna/docs/evidencias` is unchanged.

## Compatibility and protocol

- Preserve `POST /v2/exports` → 202, GET job status, health/readiness endpoints,
  `repo` / `format` / `scope`, eight-hex job IDs, and the `worker` job field.
  Jobs additionally expose persisted `attempts`, `consumer`, and attempt fencing metadata.
- Preserve main's Prometheus names and labels: `http_requests_total` (method/path/status),
  `http_request_duration_seconds` (method/path), `export_jobs_total` (Redis DBSIZE).
  The gauge retains its historical all-keys meaning, **not exact job count or queue depth**.
  Main export had no OTel module/dependencies; do not replace metrics with the older
  lab telemetry registry. Other services' team OTel instrumentation is outside this change.
- Redis standalone >=6.2, Streams `exports:jobs`, group `export-workers`, unique UUID
  consumer per worker instance. Creation is one Lua script: validate legacy lists,
  stream type and record collision, then XADD + SET. Lua does not roll back arbitrary
  runtime errors; Redis ACL/configuration and sufficient memory remain prerequisites.
- Persist the attempt before external IO. Every begin, renewal and finish checks
  XPENDING ownership atomically. Renewal/finish also require that attempt's token.
  Terminal status and XACK commit together. A stale worker cannot renew or commit,
  including between reclaim and the new owner's begin. Nonterminal retries remain pending.
- Heartbeat every lease/3; XAUTOCLAIM scans with a cursor before bounded XREADGROUP.
  Upload names `<job_id>/<lease_token>.csv` isolate stale remote IO from the winner.
  MinIO signing still uses `MINIO_PUBLIC_ENDPOINT`, `secure=False`, fixed `us-east-1`
  to avoid public-endpoint region probes. Internal writes use `MINIO_ENDPOINT`.
- Retain opt-in `EXPORT_WORKER_TEST_DELAY` while renewing the lease. SIGTERM/SIGINT
  leave interrupted work pending; clients and heartbeat are cleaned up on normal unwind.
- CSV retains the original issues columns. Non-200/invalid upstream data does not
  silently become an empty successful export. Error codes are fixed, not exception URLs.
- **PDF is not implemented**. API continues accepting `pdf` for compatibility; worker
  writes terminal `failed` / `unsupported_format`, without fetching/uploading a fake PDF.

## Mandatory upgrade runbook: drain before upgrade, otherwise ABORT

**Never roll old LPUSH/BRPOPLPUSH writers/workers alongside Streams writers/workers.**
There is no automatic or destructive migration, no queue clearing, no conversion on startup.

1. Pause old ingress/producers; keep the old worker version available. Take an operator
   backup/snapshot of the relevant Redis DB and object metadata. Do not clear Redis.
2. With the old workers, drain both `export_queue` and `export_processing` to empty.
   Let live work finish; reconcile crashed/expired-heartbeat legacy work using the old
   protocol under operator supervision. Verify every legacy queued/running job record
   has a known disposition, including a possible orphan record created by old SET-before-
   LPUSH. Empty lists alone cannot discover those orphan records.
3. Stop **all** old workers after drain; recheck both lists and the reconciliation inventory.
   Review legacy `job:heartbeat:*` and active processes to rule out a delayed old writer.
   Do not delete queued/running records or mark them done just to satisfy a gate.
4. Run the read-only preflight below with the intended Redis credentials/environment.
   A nonempty legacy list or wrong-type key is an upgrade blocker. Preserve the work and
   return to drain/reconcile with the old version; do not bypass the error or delete keys.
5. Upgrade API + workers together only after drain and preflight pass. Startup checks
   legacy state; submission checks it in the atomic creation script; readiness and each
   worker poll check again. Late legacy writes block readiness/submission/polling, but
   these checks do **not** replace quiescing old processes or reconcile orphan records.
6. If drain is not feasible, **abort upgrade** and arrange a separately reviewed,
   operator-approved migration with backups and per-job verification. Not implemented here.
   Rollback likewise requires draining/reconciling Streams pending/undelivered jobs first:
   old workers do not read Streams. Never clear a PEL to make rollback appear ready.

Read-only check from the service directory (uses existing REDIS_URL, no credentials printed):

```sh
python -c 'import os,redis; from queue_helpers import LEGACY_PREFLIGHT,LEGACY_QUEUE,LEGACY_PROCESSING; r=redis.from_url(os.environ["REDIS_URL"],decode_responses=True,socket_timeout=10,socket_connect_timeout=10); r.eval(LEGACY_PREFLIGHT,2,LEGACY_QUEUE,LEGACY_PROCESSING); r.close(); print("legacy lists drained; separately verify orphan records and stopped old processes")'
```

## Configuration

| Variable | Default | Meaning |
|---|---:|---|
| EXPORT_CLAIM_IDLE_MS | 60000 | Inactive lease before reclaim; range 1..3600000 ms. Use real IO/scheduling margin. |
| EXPORT_POLL_MS | 1000 | Bounded poll and retry-loop wait; range 1..10000 ms, never BLOCK 0. |
| EXPORT_MAX_ATTEMPTS | 3 | Persisted attempt budget, range 1..100; includes abandoned attempts. |
| EXPORT_IO_TIMEOUT_SECONDS | 10 | Redis/HTTP/MinIO connect/read timeout, >0..60 seconds; not a total payload deadline. |

## Verification and remaining limits

```sh
ARQWEB_REDIS_SERVER=/root/arqweb-runtime-tools/redis-server \
  PYTHONDONTWRITEBYTECODE=1 OTEL_EXPORTER_OTLP_ENDPOINT= \
  /tmp/arqweb-export-integration-venv/bin/python -m pytest \
  -p no:cacheprovider -v test_app.py test_integration_contract.py test_worker_recovery.py
```

Evidence outside the repository: `/root/arqweb-integration-evidence/export.log`,
`export-summary.json`, and `export-final.xml`. Original API tests are unchanged.
The Docker final stages copy `queue_helpers.py`; static packaging tests are not image builds.
Requirements are unchanged; no unrelated dependency (including Pillow elsewhere) is removed.

Real Redis fixture: private Unix socket, TCP port 0, 32MB/noeviction, bounded startup and
lifetime watchdog, owned process reaped in finally. Actual Lua/PEL ownership, atomicity,
reclaim-before/after-begin fencing, terminal replay, retry/crash budgets and API lifecycle
are exercised. Worker abandonment is simulated, **not SIGKILL**. Upstream and MinIO remain
fakes, even in the real-Redis API-to-worker case. Persistence is disabled for the fixture;
there is no claim of Redis restart/disk/power-loss durability, real MinIO/Gitea operation,
load/scaling results or complete X3/C1 acceptance. Existing FastAPI/Starlette deprecations
remain. The implementer suite is not independent final QA.

Before deployment: configure Redis volume/AOF/fsync/noeviction and ACLs, verify a real
broker restart and two real worker processes with interrupted IO, then test real object
storage and public download URLs. Redis + MinIO is not a distributed transaction.
There is no automatic retention/GC for Streams, job records, old consumers or losing
attempt artifacts. Never XTRIM/XDEL active pending entries. Plan retention and orphan
cleanup separately. Pagination, new scope behavior and signed-URL renewal remain outside scope.
