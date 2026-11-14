from fastapi.testclient import TestClient
from unittest.mock import AsyncMock
import app as app_module


def make_client():
    app_module.r = AsyncMock()
    return TestClient(app_module.app)


def test_formato_invalido_da_400():
    client = make_client()
    resp = client.post("/v2/exports", json={"repo": "equipo/repo", "format": "zip"})
    assert resp.status_code == 400


def test_crear_export_devuelve_202():
    client = make_client()
    resp = client.post("/v2/exports", json={"repo": "equipo/repo", "format": "csv"})
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"


def test_job_inexistente_da_404():
    client = make_client()
    app_module.r.get.return_value = None
    resp = client.get("/v2/exports/no-existe")
    assert resp.status_code == 404
