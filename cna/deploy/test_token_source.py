from pathlib import Path
import os
import pytest
from token_source import gitea_headers


def test_atomic_token_file_replacement_is_seen_without_restart(tmp_path, monkeypatch):
    token = tmp_path / "token"
    token.write_text("synthetic-old")
    monkeypatch.setenv("GITEA_TOKEN_FILE", str(token))
    assert gitea_headers("legacy-env") == {"Authorization": "token synthetic-old"}
    replacement = tmp_path / "next"
    replacement.write_text("synthetic-new")
    os.replace(replacement, token)
    assert gitea_headers("legacy-env") == {"Authorization": "token synthetic-new"}


@pytest.mark.parametrize("content", ["", "bad\r\nInjected", "x" * 8193])
def test_explicit_file_failure_never_falls_back_to_old_environment(tmp_path, monkeypatch, content):
    token = tmp_path / "token"
    token.write_text(content)
    monkeypatch.setenv("GITEA_TOKEN_FILE", str(token))
    with pytest.raises(ValueError) as error:
        gitea_headers("must-not-fall-back")
    assert content not in str(error.value) if content else True


def test_missing_file_fails_closed(monkeypatch):
    monkeypatch.setenv("GITEA_TOKEN_FILE", "/nonexistent-arqweb-secret/token")
    with pytest.raises(ValueError): gitea_headers("old")


def test_symlink_and_directory_sources_fail_closed(tmp_path, monkeypatch):
    target = tmp_path / "secret"
    target.write_text("synthetic-token")
    link = tmp_path / "link"
    link.symlink_to(target)
    for path in (link, tmp_path):
        monkeypatch.setenv("GITEA_TOKEN_FILE", str(path))
        with pytest.raises(ValueError): gitea_headers("old")


def test_legacy_environment_still_works_when_file_not_configured(monkeypatch):
    monkeypatch.delenv("GITEA_TOKEN_FILE", raising=False)
    assert gitea_headers("synthetic-legacy") == {"Authorization": "token synthetic-legacy"}
