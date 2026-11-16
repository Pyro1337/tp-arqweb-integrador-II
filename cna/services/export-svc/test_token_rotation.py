import os
import httpx
import worker
from test_worker_recovery import UnitQueue, UnitStore


def test_actual_worker_rereads_token_after_atomic_rotation(tmp_path, monkeypatch):
    token = tmp_path / "token"
    token.write_text("synthetic-old")
    monkeypatch.setenv("GITEA_TOKEN_FILE", str(token))
    seen = []
    def get(url, **kwargs):
        seen.append(kwargs["headers"].get("Authorization"))
        return httpx.Response(200, json=[])
    monkeypatch.setattr(worker.httpx, "get", get)
    for value in ("synthetic-old", "synthetic-new"):
        new = tmp_path / "next"
        new.write_text(value)
        os.replace(new, token)
        q, store = UnitQueue(), UnitStore()
        worker.handle_delivery(q, store, "1-0", {"job_id": q.job["job_id"]}, public_client=store)
        assert q.finished[-1][0] == "done"
    assert seen == ["token synthetic-old", "token synthetic-new"]


def test_actual_worker_does_not_call_upstream_on_invalid_token_file(tmp_path, monkeypatch):
    monkeypatch.setenv("GITEA_TOKEN_FILE", str(tmp_path / "absent"))
    calls = []
    monkeypatch.setattr(worker.httpx, "get", lambda *a, **k: calls.append(1))
    q, store = UnitQueue(), UnitStore()
    worker.handle_delivery(q, store, "1-0", {"job_id": q.job["job_id"]}, public_client=store)
    assert not calls
    assert q.finished[-1][0] == "retry"
