import ast
from pathlib import Path
import yaml

CNA = Path(__file__).resolve().parents[1]


def test_rotating_token_is_used_at_actual_gitea_call_sites_and_packaged():
    canonical = (CNA / "deploy/token_source.py").read_bytes()
    for service, module, dockerfile in (("search-svc", "app.py", "Dockerfile"),
                                        ("export-svc", "worker.py", "Dockerfile.worker")):
        directory = CNA / "services" / service
        tree = ast.parse((directory / module).read_text())
        assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                   and n.func.id == "gitea_headers" for n in ast.walk(tree))
        assert (directory / "token_source.py").read_bytes() == canonical
        assert "token_source.py" in (directory / dockerfile).read_text()
    override = yaml.safe_load((CNA / "deploy/compose.token-file.yml").read_text())["services"]
    for service in ("search-svc", "export-worker"):
        settings = override[service]
        assert settings["environment"]["GITEA_TOKEN"] == ""
        assert settings["environment"]["GITEA_TOKEN_FILE"] == "/run/secrets/gitea/token"
        mount = settings["volumes"][0]
        assert mount["target"] == "/run/secrets/gitea" and mount["read_only"]
        assert mount["bind"]["create_host_path"] is False
