"""UNIT: rotating token files through the real callback, MockTransport only."""
import asyncio

import httpx
import pytest
from opentelemetry import trace

import app as service
from resilience import GiteaResilience
from test_resilience import Message, install_http


@pytest.fixture(autouse=True)
def reset_token_handler(monkeypatch):
    monkeypatch.setattr(service, 'gitea', GiteaResilience(backoff=0))
    monkeypatch.setattr(service, 'GITEA_TOKEN', 'synthetic-env-fallback')
    service.seen_events.clear()
    yield
    service.seen_events.clear()


def test_actual_handler_reloads_atomically_replaced_token_per_upstream_attempt(monkeypatch, tmp_path):
    token = tmp_path / 'token'
    token.write_text('synthetic-first\n')
    monkeypatch.setenv('GITEA_TOKEN_FILE', str(token))
    upstream = []
    downstream = []

    def replace_token(value):
        replacement = tmp_path / 'next-token'
        replacement.write_text(value + '\n')
        replacement.replace(token)

    def handler(request):
        ctx = trace.get_current_span().get_span_context()
        assert ctx.is_valid
        assert request.headers['traceparent'] == f'00-{ctx.trace_id:032x}-{ctx.span_id:016x}-01'
        assert request.headers['x-request-id'] == 'unit-token-rotation'
        if request.url.path.startswith('/api/v1/'):
            upstream.append(request.headers['authorization'])
            if len(upstream) == 1:
                replace_token('synthetic-retry')
                return httpx.Response(503, json={})
            return httpx.Response(200, json={})
        downstream.append(request.headers['authorization'])
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 1})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)

    async def scenario():
        first = Message(event_id='rotation-first', request_id='unit-token-rotation',
                        headers={'Authorization': 'token synthetic-untrusted'})
        await service.handle_push(first)
        assert first.acks == 1
        assert upstream == ['token synthetic-first', 'token synthetic-retry']
        replace_token('synthetic-next-operation')
        second = Message(event_id='rotation-second', request_id='unit-token-rotation')
        await service.handle_push(second)
        assert second.acks == 1
        assert upstream == ['token synthetic-first', 'token synthetic-retry',
                            'token synthetic-next-operation']
        assert downstream == [f'Bearer {service.MEILI_KEY}'] * 4
        assert service.seen_events == {'rotation-first', 'rotation-second'}

    asyncio.run(scenario())


def test_actual_handler_missing_explicit_token_file_prevents_upstream_and_ack(monkeypatch, tmp_path):
    token = tmp_path / 'missing-token'
    monkeypatch.setenv('GITEA_TOKEN_FILE', str(token))
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.startswith('/api/v1/'):
            assert request.headers['authorization'] == 'token synthetic-recovered'
            return httpx.Response(200, json={})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 1})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)

    async def scenario():
        failed = Message()
        await service.handle_push(failed)
        assert requests == []
        assert failed.acks == 0 and not service.seen_events
        replacement = tmp_path / 'replacement'
        replacement.write_text('synthetic-recovered\n')
        replacement.replace(token)
        recovered = Message()  # Same event, not poisoned by the file failure.
        await service.handle_push(recovered)
        assert recovered.acks == 1 and len(requests) == 3
        assert service.seen_events == {'same-event'}

    asyncio.run(scenario())
