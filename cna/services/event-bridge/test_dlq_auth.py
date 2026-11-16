"""UNIT: dedicated opt-in authorization before fake broker reads."""
import os
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
import pytest
import app as app_module

def make_client():
    # UNIT boundaries: successful JetStream PubAck, not Core NATS publish.
    app_module.nc = SimpleNamespace(is_connected=True, publish=AsyncMock())
    app_module.js = SimpleNamespace(publish=AsyncMock(return_value=SimpleNamespace(stream="EVENTS", seq=1)))
    app_module.dlq = SimpleNamespace(ready=True, check_resources=AsyncMock(), read=AsyncMock(
        return_value={"total": 0, "items": [], "truncated": False}))
    return TestClient(app_module.app)


def test_unit_dlq_endpoint_reads_broker_and_returns_safe_503(monkeypatch):
    monkeypatch.setenv("DLQ_READ_TOKEN", "synthetic-dlq-token")
    client = make_client()
    app_module.dlq.read.return_value = {"total": 2, "items": [{"event_id": "durable"}], "truncated": True}
    response = client.get("/dlq?limit=1", headers={"X-DLQ-Token": "synthetic-dlq-token"})
    assert response.json() == app_module.dlq.read.return_value
    app_module.dlq.read.assert_awaited_once_with(limit=1)
    app_module.dlq.read.side_effect = RuntimeError("synthetic-private-url")
    response = client.get("/dlq", headers={"X-DLQ-Token": "synthetic-dlq-token"})
    assert response.status_code == 503
    assert "synthetic-private-url" not in response.text
    assert client.get("/dlq?limit=101", headers={"X-DLQ-Token": "synthetic-dlq-token"}).status_code == 422


@pytest.mark.parametrize("configured, supplied, status", [
    ("synthetic-dlq-token", None, 401),
    ("synthetic-dlq-token", "test-secret", 401),
    ("synthetic-dlq-token", "wrong", 401),
    ("", "synthetic-dlq-token", 503),
    (None, "test-secret", 503),
])
def test_dlq_auth_precedes_broker_read(monkeypatch, configured, supplied, status, caplog):
    if configured is None:
        monkeypatch.delenv("DLQ_READ_TOKEN", raising=False)
    else:
        monkeypatch.setenv("DLQ_READ_TOKEN", configured)
    client = make_client()
    app_module.dlq.read.return_value = {"payload": "private-source", "trace_context": "private-context"}
    response = client.get("/dlq", headers={} if supplied is None else {"X-DLQ-Token": supplied})
    assert response.status_code == status
    app_module.dlq.read.assert_not_awaited()
    assert "private-source" not in response.text + caplog.text
    assert "private-context" not in response.text + caplog.text
    if not configured:
        assert response.json() == {"detail": "DLQ inspection disabled"}


def test_dlq_uses_constant_time_bytes_comparison(monkeypatch):
    import hmac
    monkeypatch.setenv("DLQ_READ_TOKEN", "synthetic-dlq-token")
    client = make_client()
    with patch("hmac.compare_digest", wraps=hmac.compare_digest) as compare:
        response = client.get("/dlq", headers={"X-DLQ-Token": "synthetic-dlq-token"})
    assert response.status_code == 200
    compare.assert_called_once_with(b"synthetic-dlq-token", b"synthetic-dlq-token")
