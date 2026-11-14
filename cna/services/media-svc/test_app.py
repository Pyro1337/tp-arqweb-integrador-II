import io
from unittest.mock import MagicMock, AsyncMock
from fastapi.testclient import TestClient
import app as app_module


def make_client():
    app_module.client = MagicMock()
    app_module.public_client = MagicMock()
    app_module.public_client.presigned_get_object.return_value = "http://vm2:9000/media/abc/original"
    app_module.nc = AsyncMock()
    return TestClient(app_module.app)


def test_healthz_ok():
    client = make_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_media_no_encontrado_da_404():
    client = make_client()
    app_module.public_client.presigned_get_object.side_effect = Exception("no existe")
    resp = client.get("/v2/media/no-existe")
    assert resp.status_code == 404


def test_upload_devuelve_202_y_url():
    client = make_client()
    app_module.public_client.presigned_get_object.side_effect = None
    app_module.public_client.presigned_get_object.return_value = "http://vm2:9000/media/abc/original"

    fake_image = io.BytesIO(b"\x89PNG\r\n\x1a\n" + b"0" * 100)
    resp = client.post(
        "/v2/media",
        files={"file": ("test.png", fake_image, "image/png")},
        data={"repo": "equipo/repo", "ref_type": "issue", "ref_id": "1"},
    )
    assert resp.status_code == 202
    assert "url" in resp.json()
