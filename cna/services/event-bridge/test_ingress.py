"""UNIT/ASGI: real HTTP routing + OTel SDK, fake broker boundary."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import app
from opentelemetry import trace
from conftest import SPAN_EXPORTER


def post(client, **kwargs):
    return client.post('/webhook/test-secret', json={'action': 'opened'},
                       headers={'X-Gitea-Event': 'issues', 'X-Gitea-Delivery': 'delivery-ingress',
                                'X-Request-ID': 'request-ingress',
                                'traceparent': '00-' + '1' * 32 + '-' + '2' * 16 + '-01'}, **kwargs)


def test_ingress_waits_for_expected_puback_and_preserves_current_trace(monkeypatch):
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        async def publish(*args, **kwargs):
            entered.set()
            await release.wait()
            return SimpleNamespace(stream='EVENTS', seq=1)
        js = SimpleNamespace(publish=AsyncMock(side_effect=publish))
        core = SimpleNamespace(publish=AsyncMock())
        monkeypatch.setattr(app, 'js', js, raising=False)
        monkeypatch.setattr(app, 'nc', core)
        SPAN_EXPORTER.clear()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            task = asyncio.create_task(post(client))
            try:
                async with asyncio.timeout(1):
                    await entered.wait()
                assert not task.done(), 'HTTP success preceded PubAck'
            finally:
                release.set()
                response = await task
        assert response.status_code == 200
        assert response.json() == {'status': 'published', 'event_id': 'delivery-ingress'}
        core.publish.assert_not_awaited()
        args, kwargs = js.publish.await_args
        assert args[0] == 'gitea.issue.created'
        assert kwargs['timeout'] == 2
        envelope = json.loads(args[1])
        assert envelope['request_id'] == 'request-ingress'
        assert envelope['event_id'] == 'delivery-ingress'
        carrier = envelope['trace_context']['traceparent'].split('-')
        assert carrier[1] == '1' * 32 and carrier[2] != '2' * 16
        trace.get_tracer_provider().force_flush()
        spans = SPAN_EXPORTER.get_finished_spans()
        assert any(s.context.span_id == int(carrier[2], 16) for s in spans)
    asyncio.run(run())


@pytest.mark.parametrize('ack', [None, SimpleNamespace(stream='WRONG', seq=1),
    SimpleNamespace(stream='EVENTS', seq=0), SimpleNamespace(stream='EVENTS', seq='1')])
def test_ingress_rejects_invalid_puback(monkeypatch, ack):
    async def run():
        core = SimpleNamespace(publish=AsyncMock())
        monkeypatch.setattr(app, 'nc', core)
        monkeypatch.setattr(app, 'js', SimpleNamespace(publish=AsyncMock(return_value=ack)), raising=False)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            response = await post(client)
        assert response.status_code == 503
        assert response.json() == {'detail': 'publicacion no confirmada'}
        core.publish.assert_not_awaited()
    asyncio.run(run())


def test_ingress_timeout_is_safe_and_not_success(monkeypatch, caplog):
    async def run():
        monkeypatch.setattr(app, 'nc', SimpleNamespace(publish=AsyncMock()))
        monkeypatch.setattr(app, 'js', SimpleNamespace(publish=AsyncMock(
            side_effect=TimeoutError('nats://secret:password@broker'))), raising=False)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            response = await post(client)
        assert response.status_code == 503
        assert 'password' not in response.text + caplog.text
    asyncio.run(run())


def test_current_dashboard_metric_names_and_grpc_stack_remain():
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            await client.get('/healthz')
            response = await client.get('/metrics')
        assert response.headers['content-type'].startswith('text/plain')
        for name in ('http_requests_total', 'http_request_duration_seconds', 'dlq_messages_total'):
            assert name in response.text
        assert app.OTLPSpanExporter is not None
        assert app.FastAPIInstrumentor is not None
    asyncio.run(run())
