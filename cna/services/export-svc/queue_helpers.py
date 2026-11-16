"""Redis Streams protocol (Redis >= 6.2, standalone Redis, decoded responses)."""
import os

STREAM = "exports:jobs"
GROUP = "export-workers"
LEGACY_QUEUE = "export_queue"
LEGACY_PROCESSING = "export_processing"
IO_TIMEOUT = float(os.getenv("EXPORT_IO_TIMEOUT_SECONDS", "10"))
CLAIM_IDLE_MS = int(os.getenv("EXPORT_CLAIM_IDLE_MS", "60000"))
POLL_MS = int(os.getenv("EXPORT_POLL_MS", "1000"))
MAX_ATTEMPTS = int(os.getenv("EXPORT_MAX_ATTEMPTS", "3"))

# Fail closed, never migrate/delete legacy data. Standalone Redis only.
_LEGACY_CHECK = """
for _, key in ipairs({KEYS[#KEYS-1], KEYS[#KEYS]}) do
  local kind = redis.call('TYPE', key).ok
  if kind ~= 'none' and kind ~= 'list' then
    return redis.error_reply('invalid legacy export queue type')
  end
  if kind == 'list' and redis.call('LLEN', key) > 0 then
    return redis.error_reply('legacy export backlog: drain before upgrade')
  end
end
"""
LEGACY_PREFLIGHT = _LEGACY_CHECK + "return 1"

# Check all predictable errors BEFORE writing: Lua does not roll back runtime errors.
# KEYS: job, stream, legacy queue, legacy processing.
CREATE_JOB = _LEGACY_CHECK + """
local kind = redis.call('TYPE', KEYS[2]).ok
if kind ~= 'none' and kind ~= 'stream' then
  return redis.error_reply('invalid queue type')
end
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.error_reply('job exists')
end
local id = redis.call('XADD', KEYS[2], '*', 'job_id', ARGV[1])
redis.call('SET', KEYS[1], ARGV[2])
return id
"""


import json
import re
import uuid
from redis.exceptions import ResponseError

# The pending-entry owner AND per-attempt fencing token are checked inside the
# same Redis script as every state write/ACK. A reclaimed owner cannot commit.
BEGIN_JOB = """
local pending = redis.call('XPENDING', KEYS[2], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return nil end
local raw = redis.call('GET', KEYS[1])
local ok, job = pcall(cjson.decode, raw or '')
local function fail(code)
  redis.call('SET', KEYS[1], cjson.encode({job_id=ARGV[5],status='failed',error=code}))
  redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
  return nil
end
if not ok or type(job) ~= 'table' or job.job_id ~= ARGV[5] or
   type(job.repo) ~= 'string' or type(job.format) ~= 'string' or
   type(job.attempts or 0) ~= 'number' or (job.attempts or 0) < 0 or
   (job.attempts or 0) % 1 ~= 0 then
  return fail('invalid_job_metadata')
end
if job.status == 'done' or job.status == 'failed' then
  redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
  return nil
end
if (job.attempts or 0) >= tonumber(ARGV[6]) then
  job.status = 'failed'
  job.error = 'attempts_exhausted'
  redis.call('SET', KEYS[1], cjson.encode(job))
  redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
  return nil
end
job.attempts = (job.attempts or 0) + 1
job.status = 'running'
job.lease_token = ARGV[4]
job.consumer = ARGV[3]
job.worker = ARGV[3]
raw = cjson.encode(job)
redis.call('SET', KEYS[1], raw)
return raw
"""

FINISH_JOB = """
local pending = redis.call('XPENDING', KEYS[2], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local job = cjson.decode(raw)
if job.lease_token ~= ARGV[4] or job.status ~= 'running' then return 0 end
local extra = cjson.decode(ARGV[6])
job.status = ARGV[5]
if job.status == 'retry' and job.attempts >= tonumber(ARGV[7]) then job.status = 'failed' end
for k,v in pairs(extra) do job[k] = v end
redis.call('SET', KEYS[1], cjson.encode(job))
if job.status == 'done' or job.status == 'failed' then
  redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
end
return 1
"""


class StreamQueue:
    def __init__(self, connection, consumer=None, claim_idle_ms=CLAIM_IDLE_MS, poll_ms=POLL_MS,
                 max_attempts=MAX_ATTEMPTS):
        if not (1 <= poll_ms <= 10000 and 1 <= claim_idle_ms <= 3600000 and
                1 <= max_attempts <= 100 and 0 < IO_TIMEOUT <= 60):
            raise ValueError('invalid export queue configuration')
        self.r = connection
        self.consumer = consumer or uuid.uuid4().hex
        self.claim_idle_ms = claim_idle_ms
        self.poll_ms = poll_ms
        self.max_attempts = max_attempts
        self.claim_cursor = '0-0'
        self.preflight()
        try:
            self.r.xgroup_create(STREAM, GROUP, id='0-0', mkstream=True)
        except ResponseError as exc:
            if not str(exc).startswith('BUSYGROUP'):
                raise

    def preflight(self):
        self.r.eval(LEGACY_PREFLIGHT, 2, LEGACY_QUEUE, LEGACY_PROCESSING)

    def next_delivery(self):
        self.preflight()
        result = self.r.xautoclaim(STREAM, GROUP, self.consumer, self.claim_idle_ms,
                                  start_id=self.claim_cursor, count=1)
        self.claim_cursor = result[0]
        if result[1]:
            return result[1][0]
        result = self.r.xreadgroup(GROUP, self.consumer, {STREAM: '>'}, count=1, block=self.poll_ms)
        return result[0][1][0] if result else None

    def begin(self, message_id, fields):
        job_id = fields.get('job_id', '') if isinstance(fields, dict) else ''
        if not isinstance(job_id, str) or not re.fullmatch(r'job-[a-f0-9]{8,32}', job_id):
            job_id = 'invalid-' + message_id
        raw = self.r.eval(BEGIN_JOB, 2, 'job:' + job_id, STREAM,
                          GROUP, message_id, self.consumer, uuid.uuid4().hex, job_id, self.max_attempts)
        return json.loads(raw) if raw else None

    def finish(self, message_id, job, status, **extra):
        return bool(self.r.eval(FINISH_JOB, 2, 'job:' + job['job_id'], STREAM,
                               GROUP, message_id, self.consumer, job['lease_token'], status,
                               json.dumps(extra), self.max_attempts))



    def renew(self, message_id, job):
        return bool(self.r.eval(RENEW_JOB, 2, 'job:' + job['job_id'], STREAM,
                               GROUP, message_id, self.consumer, job['lease_token']))


RENEW_JOB = """
local pending = redis.call('XPENDING', KEYS[2], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, job = pcall(cjson.decode, raw)
if not ok or type(job) ~= 'table' or job.lease_token ~= ARGV[4] or job.status ~= 'running' then return 0 end
redis.call('XCLAIM', KEYS[2], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
return 1
"""
