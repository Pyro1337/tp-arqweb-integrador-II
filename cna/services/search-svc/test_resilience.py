"""Offline UNIT regressions, not live broker/Meili or chaos evidence."""
import asyncio
import json

import httpx
import pytest

import app as service


class Message:
    def __init__(self, **extra):
        self.data = json.dumps({'event_id': 'same-event', 'payload': {'repo': 'owner/repo'}, **extra}).encode()
        self.acks = 0

    async def ack(self):
        self.acks += 1


@pytest.fixture(autouse=True)
def reset_state(monkeypatch):
    from resilience import GiteaResilience
    monkeypatch.setattr(service, "gitea", GiteaResilience())
    service.seen_events.clear()
    yield
    service.seen_events.clear()


def install_http(monkeypatch, handler):
    original = httpx.AsyncClient
    monkeypatch.setattr(service.httpx, 'AsyncClient',
                        lambda **kw: original(transport=httpx.MockTransport(handler), **kw))


def test_failed_task_never_acknowledged(monkeypatch):
    polls = []

    def handler(request):
        if request.url.path.startswith('/api/v1/'):
            return httpx.Response(200, json={'name': 'repo'})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 7})
        polls.append(request.url.path)
        return httpx.Response(200, json={'status': 'failed'})

    install_http(monkeypatch, handler)
    msg = Message()
    asyncio.run(service.handle_push(msg))
    assert msg.acks == 0
    assert polls == ['/tasks/7']


def test_failed_delivery_does_not_poison_same_event_redelivery(monkeypatch):
    status = ['failed']
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path.startswith('/api/v1/'):
            return httpx.Response(200, json={})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 7})
        assert not service.seen_events  # No premature completed-event dedupe.
        return httpx.Response(200, json={'status': status[0]})

    install_http(monkeypatch, handler)

    async def scenario():
        first = Message()
        await service.handle_push(first)
        assert first.acks == 0 and not service.seen_events
        status[0] = 'succeeded'
        second = Message()
        await service.handle_push(second)
        assert second.acks == 1 and service.seen_events == {'same-event'}
        count = len(calls)
        duplicate = Message()
        await service.handle_push(duplicate)
        assert duplicate.acks == 1 and len(calls) == count

    asyncio.run(scenario())


def test_ack_exception_leaves_event_retryable(monkeypatch):
    def handler(request):
        if request.url.path.startswith('/api/v1/'):
            return httpx.Response(200, json={})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 7})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)

    async def scenario():
        first = Message()

        async def failed_ack():
            raise RuntimeError('ack failed')

        first.ack = failed_ack
        await service.handle_push(first)
        assert not service.seen_events
        second = Message()
        await service.handle_push(second)
        assert second.acks == 1 and service.seen_events == {'same-event'}

    asyncio.run(scenario())



@pytest.mark.parametrize('status,expected_attempts', [(401, 1), (404, 1), (429, 3), (500, 3), (503, 3)])
def test_http_errors_never_index_and_retry_only_transient(monkeypatch, status, expected_attempts):
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path.startswith('/api/v1/'):
            return httpx.Response(status, json={})
        if request.method == 'POST':
            return httpx.Response(202, json={'taskUid': 7})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)
    msg = Message()
    asyncio.run(service.handle_push(msg))
    assert msg.acks == 0 and not service.seen_events
    assert len(calls) == expected_attempts
    assert all(r.url.path.startswith('/api/v1/') for r in calls)


@pytest.mark.parametrize('status', [429, 503])
def test_transient_retries_recover_with_async_exponential_backoff(monkeypatch, status):
    attempts = []
    delays = []
    real_sleep = asyncio.sleep

    async def sleep(delay):
        delays.append(delay)
        await real_sleep(0)

    def handler(request):
        attempts.append(request)
        return httpx.Response(status if len(attempts) < 3 else 200, json={})

    install_http(monkeypatch, handler)
    monkeypatch.setattr(asyncio, 'sleep', sleep)

    async def scenario():
        async with httpx.AsyncClient() as client:
            result = await service.get_repo_con_reintentos(client, 'owner/repo')
        assert result.status_code == 200
        assert len(attempts) == 3 and delays == [0.05, 0.1]

    asyncio.run(scenario())


def test_total_deadline_bounds_stalled_transport_and_backoff(monkeypatch):
    policy = service.gitea
    policy.total_timeout = 0.02
    policy.request_timeout = 1

    async def stalled(request):
        await asyncio.Event().wait()

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(stalled)) as client:
            started = asyncio.get_running_loop().time()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(policy.get(client, 'http://unit/repo'), 0.2)
            elapsed = asyncio.get_running_loop().time() - started
            assert 0.01 <= elapsed < 0.15
        policy.backoff = 1
        calls = []

        def failed(request):
            calls.append(request)
            return httpx.Response(503, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(failed)) as client:
            started = asyncio.get_running_loop().time()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(policy.get(client, 'http://unit/repo'), 0.2)
            assert asyncio.get_running_loop().time() - started < 0.15
            assert len(calls) == 1

    asyncio.run(scenario())


def test_concurrency_cap_queue_deadline_and_cancel_release():
    policy = service.gitea
    policy.total_timeout = 1

    async def scenario():
        active = 0
        peak = 0
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def handler(request):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active >= 5:
                entered.set()
            try:
                await gate.wait()
                return httpx.Response(200, json={})
            finally:
                active -= 1

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            calls = [asyncio.create_task(policy.get(client, 'http://unit/repo')) for _ in range(9)]
            try:
                await asyncio.wait_for(entered.wait(), 0.2)
                await asyncio.sleep(0.01)
                assert peak == 5
                policy.total_timeout = 0.02
                started = asyncio.get_running_loop().time()
                with pytest.raises(asyncio.TimeoutError):
                    await policy.get(client, 'http://unit/queued')
                assert asyncio.get_running_loop().time() - started < 0.15
                assert peak == 5
                calls[0].cancel()
                await asyncio.gather(calls[0], return_exceptions=True)
            finally:
                gate.set()
                await asyncio.gather(*calls, return_exceptions=True)
            assert (await policy.get(client, 'http://unit/recovery')).status_code == 200
        assert active == 0 and peak == 5

    asyncio.run(scenario())


def test_circuit_opens_then_allows_only_one_half_open_probe(monkeypatch):
    clock = [100.0]
    policy = service.gitea
    policy.attempts = 1
    monkeypatch.setattr(policy, "clock", lambda: clock[0], raising=False)

    async def scenario():
        calls = 0
        healthy = False
        probe_entered = asyncio.Event()
        release_probe = asyncio.Event()

        async def handler(request):
            nonlocal calls
            calls += 1
            if not healthy:
                return httpx.Response(503, json={})
            probe_entered.set()
            await release_probe.wait()
            return httpx.Response(200, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for _ in range(3):
                with pytest.raises(httpx.HTTPStatusError):
                    await policy.get(client, "http://unit/repo")
            with pytest.raises(RuntimeError, match="circuit"):
                await policy.get(client, "http://unit/repo")
            assert calls == 3
            clock[0] += 31
            healthy = True
            probe = asyncio.create_task(policy.get(client, "http://unit/repo"))
            await asyncio.wait_for(probe_entered.wait(), 0.1)
            try:
                blocked = await asyncio.gather(
                    *(policy.get(client, "http://unit/repo") for _ in range(8)),
                    return_exceptions=True,
                )
                assert all(isinstance(result, RuntimeError) for result in blocked)
                assert calls == 4
            finally:
                release_probe.set()
                await probe
            assert (await policy.get(client, "http://unit/repo")).status_code == 200
            assert calls == 5

    asyncio.run(scenario())



def test_gitea_request_timeout_is_retried_within_total_budget():
    from resilience import GiteaResilience

    async def scenario():
        calls = 0
        policy = GiteaResilience(request_timeout=0.005, backoff=0)

        async def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.Event().wait()
            return httpx.Response(200, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await policy.get(client, "http://unit/repo")
            assert response.status_code == 200
            assert calls == 2

    asyncio.run(scenario())




@pytest.mark.parametrize('outcome', ['failed', 'cancelled'])
def test_half_open_failure_or_cancel_reopens_without_retries(monkeypatch, outcome):
    policy = service.gitea
    policy.attempts = 3
    clock = [100.0]
    monkeypatch.setattr(policy, 'clock', lambda: clock[0], raising=False)
    calls = []
    state = ['failed']
    entered = None

    async def handler(request):
        calls.append(request)
        if state[0] == 'cancelled':
            entered.set()
            await asyncio.Event().wait()
        return httpx.Response(503, json={})

    async def scenario():
        nonlocal entered
        entered = asyncio.Event()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for _ in range(3):
                with pytest.raises(httpx.HTTPStatusError):
                    await policy.get(client, 'http://unit/repo')
            assert len(calls) == 9
            with pytest.raises(RuntimeError, match='circuit'):
                await policy.get(client, 'http://unit/repo')
            clock[0] += 31
            state[0] = outcome
            if outcome == 'cancelled':
                task = asyncio.create_task(policy.get(client, 'http://unit/repo'))
                await asyncio.wait_for(entered.wait(), 0.2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(httpx.HTTPStatusError):
                    await policy.get(client, 'http://unit/repo')
            assert len(calls) == 10  # Half-open is a single attempt, no retry burst.
            with pytest.raises(RuntimeError, match='circuit'):
                await policy.get(client, 'http://unit/repo')
            assert len(calls) == 10

    asyncio.run(scenario())


def test_stale_success_cannot_close_newer_open_circuit(monkeypatch):
    policy = service.gitea
    policy.attempts = 1
    late_entered = None
    release = None

    async def handler(request):
        if request.url.path == '/late':
            late_entered.set()
            await release.wait()
            return httpx.Response(200, json={})
        return httpx.Response(503, json={})

    async def scenario():
        nonlocal late_entered, release
        late_entered, release = asyncio.Event(), asyncio.Event()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            late = asyncio.create_task(policy.get(client, 'http://unit/late'))
            try:
                await asyncio.wait_for(late_entered.wait(), 0.2)
                for _ in range(3):
                    with pytest.raises(httpx.HTTPStatusError):
                        await policy.get(client, 'http://unit/fail')
            finally:
                release.set()
                await late
            with pytest.raises(RuntimeError, match='circuit'):
                await policy.get(client, 'http://unit/fail')

    asyncio.run(scenario())


@pytest.mark.parametrize('stage', ['index', 'poll', 'ack'])
def test_total_handler_deadline_leaves_stalled_delivery_retryable(monkeypatch, stage):
    monkeypatch.setattr(service, 'HANDLER_TIMEOUT', 0.02, raising=False)

    async def handler(request):
        current = 'upstream' if request.url.path.startswith('/api/v1/') else ('index' if request.method == 'POST' else 'poll')
        if current == stage:
            await asyncio.Event().wait()
        if current == 'upstream':
            return httpx.Response(200, json={})
        if current == 'index':
            return httpx.Response(202, json={'taskUid': 7})
        return httpx.Response(200, json={'status': 'succeeded'})

    install_http(monkeypatch, handler)
    msg = Message()
    if stage == 'ack':
        async def ack():
            await asyncio.Event().wait()
        msg.ack = ack

    async def scenario():
        started = asyncio.get_running_loop().time()
        result = await asyncio.gather(asyncio.wait_for(service.handle_push(msg), 0.2), return_exceptions=True)
        assert result == [None]  # Service timeout, not the safety watchdog.
        assert 0.01 <= asyncio.get_running_loop().time() - started < 0.15
        assert msg.acks == 0 and not service.seen_events

    asyncio.run(scenario())
