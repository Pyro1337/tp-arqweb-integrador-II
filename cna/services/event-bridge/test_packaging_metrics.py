"""UNIT: packaging/depth contracts (not a Docker build)."""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
from prometheus_client.parser import text_string_to_metric_families
import pytest
import app


@pytest.mark.parametrize('depth', [0, 7])
def test_dashboard_dlq_gauge_is_broker_depth(monkeypatch, depth):
    async def run():
        info = AsyncMock(return_value=SimpleNamespace(state=SimpleNamespace(messages=depth)))
        monkeypatch.setattr(app, 'js', SimpleNamespace(stream_info=info))
        monkeypatch.setattr(app, 'nc', SimpleNamespace(is_connected=True))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            response = await client.get('/metrics')
        samples = {s.name: s.value for m in text_string_to_metric_families(response.text) for s in m.samples}
        assert samples['dlq_messages_total'] == depth
        info.assert_awaited_once_with('BRIDGE_DLQ')
    asyncio.run(run())


def test_dashboard_dlq_unknown_is_not_false_zero(monkeypatch):
    import math
    async def run():
        monkeypatch.setattr(app, 'js', SimpleNamespace(stream_info=AsyncMock(side_effect=TimeoutError())))
        monkeypatch.setattr(app, 'nc', SimpleNamespace(is_connected=True))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            response = await client.get('/metrics')
        samples = {s.name: s.value for m in text_string_to_metric_families(response.text) for s in m.samples}
        assert math.isnan(samples['dlq_messages_total'])
    asyncio.run(run())


def test_final_image_contains_helper_and_installs_current_requirements():
    root = Path(__file__).parent
    dockerfile = (root / 'Dockerfile').read_text()
    final = dockerfile.rsplit('FROM ', 1)[1]
    assert 'COPY app.py newdlq.py ./' in final
    assert 'pip install --no-cache-dir -r requirements.txt' in dockerfile
    assert 'COPY --from=base /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages' in final
    assert 'opentelemetry-exporter-otlp-proto-grpc==1.27.0' in (root / 'requirements.txt').read_text()
    assert 'nats-py==2.9.0' in (root / 'requirements.txt').read_text()
