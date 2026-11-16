"""UNIT boundary integration: existing SDK spans, mocked HTTP and NATS only."""
import asyncio
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client.parser import text_string_to_metric_families

import app as service
from test_resilience import Message, install_http
from resilience import GiteaResilience

PARENT = '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01'


@pytest.fixture
def exporter(monkeypatch):
    provider = TracerProvider()
    exported = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exported))
    monkeypatch.setattr(service, 'tracer', provider.get_tracer('search-svc'))
    monkeypatch.setattr(service, 'gitea', GiteaResilience())
    service.seen_events.clear()
    yield exported
    service.seen_events.clear()
    provider.shutdown()


@pytest.mark.parametrize('carrier', ['canonical', 'top-level', 'nested'])
def test_parent_child_outgoing_context_and_ack_lifetime(monkeypatch, exporter, carrier):
    requests, active = [], []

    def handler(request):
        requests.append(request)
        active.append(trace.get_current_span().get_span_context())
        if request.url.path.startswith('/api/v1/'):
            return httpx.Response(200, json={})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 1})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)
    if carrier == 'canonical':
        extra = {'request_id': 'unit-request', 'trace_context': {'traceparent': PARENT},
                 'traceparent': '00-' + 'a' * 32 + '-' + 'b' * 16 + '-01'}
    elif carrier == 'top-level':
        extra = {'request_id': 'unit-request', 'traceparent': PARENT}
    else:
        extra = {'headers': {'X-Request-ID': 'unit-request', 'traceparent': PARENT}}
    msg = Message(**extra)
    ack_context = []

    async def ack():
        ack_context.append(trace.get_current_span().get_span_context())
        assert not service.seen_events
        msg.acks += 1

    msg.ack = ack
    asyncio.run(service.handle_push(msg))
    spans = exporter.get_finished_spans()
    consumer, = [s for s in spans if s.name == 'search-svc.handle_push']
    assert consumer.parent.span_id == int(PARENT.split('-')[2], 16)
    assert consumer.context.trace_id == int(PARENT.split('-')[1], 16)
    children = [s for s in spans if s.name != consumer.name]
    assert {s.name for s in children} == {'search-svc.consultar_gitea_api', 'search-svc.indexar_meilisearch'}
    assert all(s.parent.span_id == consumer.context.span_id for s in children)
    assert len(requests) == 3 and msg.acks == 1
    assert ack_context[0].span_id == consumer.context.span_id
    for request, ctx in zip(requests, active):
        assert request.headers['x-request-id'] == 'unit-request'
        assert request.headers['traceparent'] == f'00-{ctx.trace_id:032x}-{ctx.span_id:016x}-01'
        assert ctx.trace_id == consumer.context.trace_id
        assert ctx.span_id in {s.context.span_id for s in children}
    assert requests[0].headers['traceparent'] != PARENT
    assert requests[1].headers['traceparent'] == requests[2].headers['traceparent']
    assert requests[1].headers['Authorization'] == f'Bearer {service.MEILI_KEY}'
    assert not trace.get_current_span().get_span_context().is_valid


@pytest.mark.parametrize('stage', ['upstream', 'index', 'poll', 'ack'])
def test_caught_errors_mark_consumer_error_without_exception_secrets(monkeypatch, exporter, caplog, stage):
    secret = 'http://user:password@private/?token=topsecret'

    def handler(request):
        current = 'upstream' if request.url.path.startswith('/api/v1/') else ('index' if request.method == 'POST' else 'poll')
        if current == stage:
            raise RuntimeError(secret)
        if current == 'upstream':
            return httpx.Response(200, json={})
        if current == 'index':
            return httpx.Response(202, json={'taskUid': 1})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)
    msg = Message(request_id='unit-request', trace_context={'traceparent': PARENT})
    if stage == 'ack':
        msg.ack = AsyncMock(side_effect=RuntimeError(secret))
    with caplog.at_level('INFO', logger='search-svc'):
        asyncio.run(service.handle_push(msg))
    consumer, = [s for s in exporter.get_finished_spans() if s.name == 'search-svc.handle_push']
    assert consumer.status.status_code.name == 'ERROR'
    assert not service.seen_events and msg.acks == 0
    records = [r.message for r in caplog.records if r.name == 'search-svc']
    exported_text = str([(s.attributes, s.events) for s in exporter.get_finished_spans()])
    assert records
    assert 'password' not in str(records) + exported_text
    assert 'topsecret' not in str(records) + exported_text


def test_public_metrics_contract_and_fastapi_instrumentation_preserved():
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=service.app), base_url='http://unit') as client:
            assert (await client.get('/healthz')).status_code == 200
            response = await client.get('/metrics')
        assert response.headers['content-type'].startswith('text/plain')
        samples = [s for f in text_string_to_metric_families(response.text) for s in f.samples]
        assert any(s.name == 'http_requests_total' and s.labels == {
            'method': 'GET', 'path': '/healthz', 'status': '200'} and s.value >= 1 for s in samples)
        assert any(s.name == 'http_request_duration_seconds_count' and s.labels == {
            'method': 'GET', 'path': '/healthz'} for s in samples)
        assert service.app._is_instrumented_by_opentelemetry
        assert len([r for r in service.app.routes if r.path == '/metrics']) == 1
    asyncio.run(scenario())


def test_final_docker_stage_copies_imported_local_helpers():
    final_stage = Path(__file__).with_name('Dockerfile').read_text().split('FROM ')[-1]
    assert any(line.startswith('COPY ') and 'app.py' in line and 'resilience.py' in line
               and line.endswith(' ./') for line in final_stage.splitlines())


def test_shutdown_closes_owned_nats_and_resets_readiness(monkeypatch):
    nc = type('NC', (), {'is_closed': False, 'close': AsyncMock()})()
    monkeypatch.setattr(service, 'nc', nc)
    service.ready.update(meili=True, nats=True)
    asyncio.run(service.app.router.shutdown())
    nc.close.assert_awaited_once()
    assert service.nc is None and service.ready == {'meili': False, 'nats': False}


@pytest.mark.parametrize('request_id', ['bad secret value', 'bad\r\nInjected: secret', {'bad': 'secret'}, 'x' * 65, None])
def test_request_correlation_is_validated_before_forwarding(monkeypatch, exporter, request_id):
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.startswith('/api/v1/'):
            return httpx.Response(200, json={})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 1})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)
    msg = Message(request_id=request_id)
    asyncio.run(service.handle_push(msg))
    assert msg.acks == 1 and len(requests) == 3
    forwarded = [r.headers.get('x-request-id') for r in requests]
    assert all(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', value) for value in forwarded)
    assert len(set(forwarded)) == 1


@pytest.mark.parametrize('data', [b'not json', b'[]', b'null'])
def test_invalid_envelope_stays_unacked_without_callback_exception(data):
    msg = Message()
    msg.data = data
    asyncio.run(service.handle_push(msg))
    assert msg.acks == 0


def test_main_circuit_configuration_is_retained():
    assert service.gitea.failure_threshold == service.CIRCUIT_FAILURE_THRESHOLD
    assert service.gitea.cooldown == service.CIRCUIT_COOLDOWN
