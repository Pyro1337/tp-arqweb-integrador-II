import json
import logging
from unittest.mock import AsyncMock

import pytest
from opentelemetry import trace
import app as module


@pytest.mark.asyncio
async def test_consumer_logs_request_and_active_trace_on_success(caplog, monkeypatch):
    class Pool:
        def acquire(self): return self
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def fetchval(self, *args): return "event-1"
    monkeypatch.setattr(module, "pool", Pool())
    envelope = {"event_id": "event-1", "type": "gitea.push",
                "occurred_at": "2026-11-16T00:00:00+00:00",
                "request_id": "req-correlated", "payload": {"repo": "team/demo"},
                "trace_context": {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}}
    msg = AsyncMock(data=json.dumps(envelope).encode())
    with caplog.at_level(logging.INFO, logger="audit-svc"):
        await module.handle_event(msg)
    entry = next(json.loads(r.message) for r in caplog.records if 'registro guardado' in r.message)
    assert entry.get("request_id") == "req-correlated"
    assert entry.get("trace_id") == "1" * 32
    msg.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_consumer_failure_is_safe_correlated_and_retryable(caplog, monkeypatch):
    class Pool:
        def acquire(self): return self
        async def __aenter__(self): raise RuntimeError("do-not-log-private-db-credential")
        async def __aexit__(self, *args): pass
    monkeypatch.setattr(module, "pool", Pool())
    envelope = {"event_id": "event-2", "request_id": "req-failed", "payload": {},
                "trace_context": {"traceparent": "00-" + "3" * 32 + "-" + "4" * 16 + "-01"}}
    msg = AsyncMock(data=json.dumps(envelope).encode())
    with caplog.at_level(logging.INFO, logger="audit-svc"):
        await module.handle_event(msg)
    logs = "\n".join(r.message for r in caplog.records)
    assert "do-not-log-private-db-credential" not in logs
    entry = next(json.loads(r.message) for r in caplog.records if 'fallo al procesar evento' in r.message)
    assert entry.get("request_id") == "req-failed"
    assert entry.get("trace_id") == "3" * 32
    msg.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_envelope_is_left_unacked_without_crash():
    msg = AsyncMock(data=b"not-json")
    await module.handle_event(msg)
    msg.ack.assert_not_awaited()
