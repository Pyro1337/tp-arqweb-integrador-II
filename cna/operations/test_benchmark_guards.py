import json
import pytest
import benchmark_exports as module


def test_plan_never_queries_docker_or_http(monkeypatch, capsys):
    def forbidden(*a, **kw): raise AssertionError("external IO in plan")
    monkeypatch.setattr(module, "workers", forbidden)
    monkeypatch.setattr(module.urllib.request, "build_opener", forbidden)
    assert module.main([]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "PLAN_ONLY"


@pytest.mark.parametrize("args", [["--execute"],
    ["--execute", "--isolated-lab", "--base-url", "https://example.com"],
    ["--execute", "--isolated-lab", "--base-url", "http://127.0.0.1:8004/?token=private"],
    ["--execute", "--isolated-lab", "--count", "101"]])
def test_execution_refuses_unsafe_targets_before_external_io(monkeypatch, args):
    def forbidden(*a, **kw): raise AssertionError("external IO before guards")
    monkeypatch.setattr(module, "workers", forbidden)
    with pytest.raises(SystemExit): module.main(args)


def test_shared_docker_socket_is_rejected_before_listing_containers():
    class Result:
        stdout = json.dumps([{"Endpoints": {"docker": {"Host": "unix:///var/run/docker.sock"}}}])
    calls = []
    def runner(args, **kw):
        calls.append(args)
        return Result()
    with pytest.raises(ValueError): module.workers("arqweb-lab", "arqweb-lab", runner)
    assert len(calls) == 1
