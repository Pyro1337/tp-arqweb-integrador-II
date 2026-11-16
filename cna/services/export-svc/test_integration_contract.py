"""Integration contracts for current-main export API and packaging."""
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
import app as api


def test_atomic_submit_does_not_split_record_and_enqueue():
    api.r = AsyncMock()
    api.r.eval.return_value = "1-0"
    response = TestClient(api.app).post(
        "/v2/exports", json={"repo": "team/repo", "format": "csv"})
    assert response.status_code == 202
    assert api.r.eval.await_count == 1, "job record and queue delivery must be created atomically"
    assert api.r.set.await_count == api.r.lpush.await_count == 0
    assert response.json()["attempts"] == 0



def test_main_metrics_contract_is_preserved():
    api.r = AsyncMock()
    api.r.dbsize.return_value = 7
    client = TestClient(api.app)
    assert client.get("/healthz").json() == {"status": "ok"}
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert 'http_requests_total{method="GET",path="/healthz",status="200"}' in response.text
    assert 'http_request_duration_seconds_bucket{le=' in response.text
    assert 'export_jobs_total 7.0' in response.text


def test_public_signer_retains_main_endpoint_without_region_probe(monkeypatch):
    import worker
    from unittest.mock import Mock
    factory = Mock()
    monkeypatch.setattr(worker, "Minio", factory)
    monkeypatch.setattr(worker, "MINIO_PUBLIC_ENDPOINT", "public.invalid:9000")
    worker.get_minio_public_client()
    assert factory.call_args.args == ("public.invalid:9000",)
    assert factory.call_args.kwargs["region"] == "us-east-1"
    assert factory.call_args.kwargs["secure"] is False


def test_final_docker_stages_package_local_queue_helper():
    from pathlib import Path
    root = Path(__file__).parent
    for role in ("api", "worker"):
        final = (root / ("Dockerfile." + role)).read_text().split("FROM ")[-1]
        copies = [line for line in final.splitlines() if line.startswith("COPY ")]
        assert any("queue_helpers.py" in line and line.endswith("./") for line in copies)
