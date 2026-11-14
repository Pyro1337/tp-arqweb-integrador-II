from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
import app as app_module


def make_client():
    app_module.ready["meili"] = True
    app_module.ready["nats"] = True
    return TestClient(app_module.app)


def test_search_sin_query_da_400():
    client = make_client()
    resp = client.get("/v2/search")
    assert resp.status_code == 422  # FastAPI valida el parametro requerido


def test_readyz_503_si_no_esta_listo():
    client = make_client()
    app_module.ready["meili"] = False
    resp = client.get("/readyz")
    assert resp.status_code == 503
    app_module.ready["meili"] = True


def test_healthz_ok():
    client = make_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200


@patch("app.httpx.AsyncClient")
def test_search_devuelve_resultados(mock_client_cls):
    client = make_client()
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"hits": [{"title": "nginx-configs"}]}

    mock_instance = AsyncMock()
    mock_instance.post.return_value = fake_response
    mock_client_cls.return_value.__aenter__.return_value = mock_instance

    resp = client.get("/v2/search?q=nginx")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1
