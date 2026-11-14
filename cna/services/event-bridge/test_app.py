import os
os.environ.setdefault("WEBHOOK_SECRET", "test-secret")

from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
import app as app_module


def make_client():
    app_module.nc = AsyncMock()
    return TestClient(app_module.app)


def test_webhook_rechaza_secreto_invalido():
    client = make_client()
    resp = client.post("/webhook/secreto-incorrecto", json={})
    assert resp.status_code == 401


def test_webhook_acepta_push_valido():
    client = make_client()
    body = {
        "repository": {"full_name": "equipo/repo"},
        "ref": "refs/heads/main",
        "pusher": {"login": "arqweb2026"},
        "commits": [{"id": "abc"}],
    }
    resp = client.post("/webhook/test-secret", json=body, headers={"X-Gitea-Event": "push"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "published"


def test_healthz_ok():
    client = make_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200
