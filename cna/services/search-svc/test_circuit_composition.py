"""Offline UNIT repair regressions: real callback/SDK; HTTP and ACK are fakes."""
import asyncio

import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import app as service
from resilience import GiteaResilience
from test_resilience import Message, install_http


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.delenv('GITEA_TOKEN_FILE', raising=False)
    monkeypatch.setattr(service, 'GITEA_TOKEN', 'synthetic-fallback')
    service.seen_events.clear()
    yield
    service.seen_events.clear()


def healthy(request):
    if request.url.path.startswith('/api/v1/'):
        return httpx.Response(200, json={})
    if request.method == 'POST':
        return httpx.Response(202, json={'taskUid': 1})
    return httpx.Response(200, json={'status': 'succeeded'})


def test_actual_half_open_token_repair_obeys_new_cooldown_and_safe_boundary(monkeypatch, tmp_path, caplog):
    token = tmp_path / 'token'
    token.write_text('synthetic-initial')
    monkeypatch.setenv('GITEA_TOKEN_FILE', str(token))
    policy = GiteaResilience(attempts=1, backoff=0, cooldown=service.CIRCUIT_COOLDOWN)
    now = [100.0]
    policy.clock = lambda: now[0]
    monkeypatch.setattr(service, 'gitea', policy)
    provider, exported = TracerProvider(), InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exported))
    monkeypatch.setattr(service, 'tracer', provider.get_tracer('repair'))
    calls, state = [], ['failing']

    def handler(request):
        calls.append(request)
        if state[0] == 'failing':
            return httpx.Response(503, json={})
        if request.url.path.startswith('/api/v1/'):
            assert request.headers['authorization'] == 'token synthetic-repaired'
        return healthy(request)

    install_http(monkeypatch, handler)

    async def scenario():
        for _ in range(3):
            msg = Message()
            await service.handle_push(msg)
            assert msg.acks == 0 and not service.seen_events
        assert policy._opened_at == 100 and policy._failures == 3 and len(calls) == 3
        now[0] = 115  # The actual 15-second boundary admits a probe.
        token.write_text('synthetic-private\ninvalid')
        bad = Message()
        await service.handle_push(bad)
        assert bad.acks == 0 and not service.seen_events and len(calls) == 3
        assert not policy._probe_in_flight
        assert policy._opened_at == 115 and policy._failures == 3
        replacement = tmp_path / 'replacement'
        replacement.write_text('synthetic-repaired')
        replacement.replace(token)
        state[0] = 'healthy'
        now[0] = 129.999
        blocked = Message()
        await service.handle_push(blocked)
        assert blocked.acks == 0 and not service.seen_events and len(calls) == 3
        now[0] = 130
        recovered = Message()  # Same event through poll completion and ACK.
        await service.handle_push(recovered)
        assert recovered.acks == 1 and len(calls) == 6
        assert service.seen_events == {'same-event'}
        assert policy._opened_at is None and not policy._probe_in_flight and policy._failures == 0

    try:
        asyncio.run(scenario())
        spans = exported.get_finished_spans()
        assert spans and service.app._is_instrumented_by_opentelemetry
        assert 'synthetic-private' not in caplog.text + str([(s.attributes, s.events, s.status) for s in spans])
    finally:
        provider.shutdown()


@pytest.mark.parametrize('where', ['headers', 'client'])
@pytest.mark.parametrize('error_type', [ValueError, RuntimeError, TypeError])
def test_unexpected_probe_exceptions_reopen_without_counting_or_retry(where, error_type):
    policy = GiteaResilience(attempts=3, backoff=0, cooldown=15)
    now = [100.0]
    policy.clock = lambda: now[0]
    calls = []
    mode = ['fail']

    def headers():
        if mode[0] == 'unexpected' and where == 'headers':
            raise error_type('synthetic-secret')
        return {}

    def handler(request):
        calls.append(request)
        if mode[0] == 'unexpected' and where == 'client':
            raise error_type('synthetic-secret')
        return httpx.Response(503 if mode[0] == 'fail' else 200, json={})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for _ in range(3):
                with pytest.raises(httpx.HTTPStatusError):
                    await policy.get(client, 'http://unit/repo', headers)
            assert len(calls) == 9 and policy._failures == 3
            now[0] = 115
            mode[0] = 'unexpected'
            with pytest.raises(error_type, match='synthetic-secret'):
                await policy.get(client, 'http://unit/repo', headers)
            assert not policy._probe_in_flight and policy._opened_at == 115
            assert policy._failures == 3 and len(calls) == (9 if where == 'headers' else 10)
            now[0] = 129
            with pytest.raises(RuntimeError, match='circuit open'):
                await policy.get(client, 'http://unit/repo', headers)
            now[0] = 130
            mode[0] = 'healthy'
            assert (await policy.get(client, 'http://unit/repo', headers)).status_code == 200

    asyncio.run(scenario())


def test_actual_main_deadlines_open_exactly_on_third_callback_and_recover(monkeypatch):
    policy = GiteaResilience(request_timeout=service.GITEA_TIMEOUT,
                             failure_threshold=service.CIRCUIT_FAILURE_THRESHOLD,
                             cooldown=service.CIRCUIT_COOLDOWN)
    assert (policy.request_timeout, policy.total_timeout, policy.attempts,
            policy.failure_threshold, policy.cooldown, policy.backoff) == (1.5, 2, 3, 3, 15, .05)
    assert service.HANDLER_TIMEOUT == 9
    now, state, calls = [100.0], ['stalled'], []
    policy.clock = lambda: now[0]
    monkeypatch.setattr(service, 'gitea', policy)

    async def handler(request):
        calls.append(request)
        if state[0] == 'stalled':
            await asyncio.Event().wait()
        return healthy(request)

    install_http(monkeypatch, handler)

    async def scenario():
        for n in range(1, 4):
            start = asyncio.get_running_loop().time()
            msg = Message()
            assert await asyncio.wait_for(service.handle_push(msg), 3) is None
            assert 1.9 <= asyncio.get_running_loop().time() - start < 3
            assert msg.acks == 0 and not service.seen_events
            assert len(calls) == 2 * n  # Retry attempts are not separate failures.
            assert policy._failures == n
            assert policy._opened_at == (100 if n == 3 else None)
        state[0] = 'healthy'
        now[0] = 114.999
        blocked = Message()
        await service.handle_push(blocked)
        assert len(calls) == 6 and blocked.acks == 0 and not service.seen_events
        now[0] = 115
        recovered = Message()
        await service.handle_push(recovered)
        assert recovered.acks == 1 and len(calls) == 9
        assert service.seen_events == {'same-event'} and policy._failures == 0

    asyncio.run(scenario())


@pytest.mark.parametrize('outcome', ['attempt-exhaustion', 'total-in-flight', 'total-backoff'])
def test_one_failed_logical_operation_is_counted_once(outcome):
    policy = GiteaResilience(attempts=3, request_timeout=.005, backoff=0,
                             total_timeout=.2, cooldown=15)
    calls = []
    if outcome == 'total-in-flight':
        policy.request_timeout, policy.total_timeout = 1, .025
    elif outcome == 'total-backoff':
        policy.backoff, policy.total_timeout = 1, .025

    async def handler(request):
        calls.append(request)
        if outcome == 'total-in-flight':
            await asyncio.Event().wait()
        return httpx.Response(503, json={})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises((httpx.HTTPStatusError, asyncio.TimeoutError)):
                await policy.get(client, 'http://unit/repo')
        assert policy._failures == 1 and policy._opened_at is None
        assert len(calls) == (3 if outcome == 'attempt-exhaustion' else 1)

    asyncio.run(scenario())


def test_concurrent_queue_deadline_and_external_cancels_do_not_count():
    policy = GiteaResilience(attempts=1, request_timeout=1, total_timeout=1, cooldown=15)

    async def scenario():
        entered, gate = asyncio.Event(), asyncio.Event()
        calls, tasks = [], []

        async def handler(request):
            calls.append(request.url.path)
            if request.url.path == '/fail':
                return httpx.Response(503, json={})
            if len(calls) == 6:
                entered.set()
            await gate.wait()
            return httpx.Response(200, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await policy.get(client, 'http://unit/fail')
            try:
                tasks = [asyncio.create_task(policy.get(client, 'http://unit/held')) for _ in range(5)]
                await asyncio.wait_for(entered.wait(), .2)
                policy.total_timeout = .025
                queued = asyncio.create_task(policy.get(client, 'http://unit/queued-deadline'))
                cancelled_queue = asyncio.create_task(policy.get(client, 'http://unit/queued-cancel'))
                tasks.extend([queued, cancelled_queue])
                await asyncio.sleep(.005)
                assert not cancelled_queue.done()
                cancelled_queue.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await cancelled_queue
                with pytest.raises(asyncio.TimeoutError):
                    await queued
                assert len(calls) == 6 and policy._failures == 1 and policy._opened_at is None
                tasks[0].cancel()  # Externally cancelled ADMITTED operation is not an outage.
                with pytest.raises(asyncio.CancelledError):
                    await tasks[0]
                assert policy._failures == 1 and policy._opened_at is None
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                gate.set()
            assert policy._failures == 1 and not policy._probe_in_flight
            assert (await policy.get(client, 'http://unit/recovered')).status_code == 200
            assert policy._failures == 0

    asyncio.run(scenario())


@pytest.mark.parametrize('outcome', ['deadline', 'external-cancel', 'success', 'permanent', 'unexpected'])
def test_stale_operation_cannot_mutate_new_half_open_probe(outcome):
    policy = GiteaResilience(attempts=1, request_timeout=1, total_timeout=.08, cooldown=15)
    now = [100.0]
    policy.clock = lambda: now[0]

    async def scenario():
        old_entered, probe_entered, old_gate, probe_gate = [asyncio.Event() for _ in range(4)]

        async def handler(request):
            if request.url.path == '/old':
                old_entered.set()
                await old_gate.wait()
                if outcome == 'unexpected':
                    raise ValueError('synthetic-secret')
                return httpx.Response(401 if outcome == 'permanent' else 200, json={})
            if request.url.path == '/probe':
                probe_entered.set()
                await probe_gate.wait()
                return httpx.Response(200, json={})
            return httpx.Response(503, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            old = asyncio.create_task(policy.get(client, 'http://unit/old'))
            probe = None
            try:
                await asyncio.wait_for(old_entered.wait(), .2)
                for _ in range(3):
                    with pytest.raises(httpx.HTTPStatusError):
                        await policy.get(client, 'http://unit/fail')
                assert policy._opened_at == 100 and policy._generation == 1
                now[0] = 115
                policy.total_timeout = 1
                probe = asyncio.create_task(policy.get(client, 'http://unit/probe'))
                await asyncio.wait_for(probe_entered.wait(), .2)
                snapshot = (policy._generation, policy._opened_at, policy._failures, policy._probe_in_flight)
                if outcome == 'external-cancel':
                    old.cancel()
                elif outcome != 'deadline':
                    old_gate.set()
                result, = await asyncio.gather(old, return_exceptions=True)
                expected = {'deadline': asyncio.TimeoutError, 'external-cancel': asyncio.CancelledError,
                            'permanent': httpx.HTTPStatusError, 'unexpected': ValueError}
                if outcome in expected:
                    assert isinstance(result, expected[outcome])
                else:
                    assert result.status_code == 200
                assert (policy._generation, policy._opened_at, policy._failures, policy._probe_in_flight) == snapshot
                with pytest.raises(RuntimeError, match='circuit open'):
                    await policy.get(client, 'http://unit/blocked')
                probe_gate.set()
                assert (await probe).status_code == 200
                assert policy._opened_at is None and policy._failures == 0 and not policy._probe_in_flight
            finally:
                old_gate.set()
                probe_gate.set()
                await asyncio.gather(*(t for t in (old, probe) if t is not None), return_exceptions=True)

    asyncio.run(scenario())


@pytest.mark.parametrize('outcome', ['deadline', 'external-cancel', 'permanent'])
def test_half_open_deadline_counts_once_but_cancel_and_permanent_do_not(outcome):
    policy = GiteaResilience(attempts=3, request_timeout=1, total_timeout=1,
                             backoff=0, cooldown=15)
    now, mode, calls = [100.0], ['fail'], []
    policy.clock = lambda: now[0]

    async def scenario():
        entered = asyncio.Event()

        async def handler(request):
            calls.append(request)
            if mode[0] == 'fail':
                return httpx.Response(503, json={})
            if mode[0] == 'permanent':
                return httpx.Response(401, json={})
            entered.set()
            await asyncio.Event().wait()

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for _ in range(3):
                with pytest.raises(httpx.HTTPStatusError):
                    await policy.get(client, 'http://unit/repo')
            assert policy._failures == 3 and len(calls) == 9
            now[0], mode[0] = 115, outcome
            policy.total_timeout = .025
            task = asyncio.create_task(policy.get(client, 'http://unit/repo'))
            try:
                if outcome == 'external-cancel':
                    await asyncio.wait_for(entered.wait(), .2)
                    task.cancel()
                result, = await asyncio.gather(task, return_exceptions=True)
                expected = {'deadline': asyncio.TimeoutError, 'external-cancel': asyncio.CancelledError,
                            'permanent': httpx.HTTPStatusError}
                assert isinstance(result, expected[outcome])
                assert len(calls) == 10 and not policy._probe_in_flight
                if outcome == 'permanent':
                    assert policy._failures == 0 and policy._opened_at is None
                else:
                    assert policy._failures == (4 if outcome == 'deadline' else 3)
                    assert policy._opened_at == 115
                    with pytest.raises(RuntimeError, match='circuit open'):
                        await policy.get(client, 'http://unit/repo')
                    assert len(calls) == 10
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


@pytest.mark.parametrize('outcome', ['permanent', 'unexpected'])
def test_closed_permanent_or_unexpected_error_is_not_a_transient_failure(outcome):
    policy = GiteaResilience(attempts=1)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) == 1 else 401, json={})

    def headers():
        if outcome == 'unexpected':
            raise ValueError('synthetic-secret')
        return {}

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await policy.get(client, 'http://unit/repo')
            assert policy._failures == 1
            with pytest.raises(ValueError if outcome == 'unexpected' else httpx.HTTPStatusError):
                await policy.get(client, 'http://unit/repo', headers)
            assert policy._opened_at is None and not policy._probe_in_flight
            assert policy._failures == (1 if outcome == 'unexpected' else 0)
            assert len(calls) == (1 if outcome == 'unexpected' else 2)

    asyncio.run(scenario())
