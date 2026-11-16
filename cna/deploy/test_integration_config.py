from pathlib import Path
import json
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_dedicated_dlq_credential_is_wired_only_to_bridge():
    services = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text())["services"]
    assert services["event-bridge"]["environment"].get("DLQ_READ_TOKEN") == "${DLQ_READ_TOKEN:-}"
    assert all("DLQ_READ_TOKEN" not in s.get("environment", {}) for name, s in services.items() if name != "event-bridge")


def test_export_redis_uses_aof_not_implicit_snapshot_only():
    redis = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text())["services"]["export-redis"]
    assert redis.get("command") == ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"]


def test_queue_exporter_is_scraped_and_dashboard_displays_waiting_and_inflight():
    config = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text())
    assert "queue-metrics" in config["services"]
    prom = yaml.safe_load((ROOT / "observability/prometheus/prometheus.yml").read_text())
    assert any(c["job_name"] == "queue-metrics" for c in prom["scrape_configs"])
    dashboard = json.loads((ROOT / "observability/grafana/provisioning/dashboards/red-dashboard.json").read_text())
    expressions = str(dashboard)
    assert "queue_waiting_messages" in expressions
    assert "queue_inflight_messages" in expressions
    assert "queue_collector_up" in expressions


def test_every_explicit_dashboard_datasource_uid_is_provisioned():
    sources = yaml.safe_load((ROOT / "observability/grafana/provisioning/datasources/datasources.yml").read_text())
    known = {s.get("uid") for s in sources["datasources"]}
    dashboard = json.loads((ROOT / "observability/grafana/provisioning/dashboards/red-dashboard.json").read_text())
    for panel in dashboard["panels"]:
        if isinstance(panel.get("datasource"), dict):
            assert panel["datasource"]["uid"] in known


def test_gateway_logs_correlation_without_query_secrets():
    config = (ROOT / "deploy/nginx-api.equipo01.arqweb.test.conf").read_text()
    assert 'log_format cna_json escape=json' in config
    assert 'access_log /var/log/nginx/cna-access.json cna_json;' in config
    assert 'proxy_set_header traceparent $http_traceparent;' in config
    log_format = config.split('log_format cna_json escape=json', 1)[1].split(';', 1)[0]
    assert '$args' not in log_format and '$request_uri' not in log_format
    assert '$req_id' in log_format
