"""Process-local async resilience primitives (not a durable event ledger)."""
import asyncio
import time

import httpx


class GiteaResilience:
    """Bounded retries for idempotent GETs; never retry permanent HTTP failures."""
    def __init__(self, attempts=3, request_timeout=0.5, backoff=0.05, total_timeout=2.0,
                 failure_threshold=3, cooldown=30.0):
        if attempts < 1 or min(request_timeout, total_timeout) <= 0 or backoff < 0:
            raise ValueError("invalid retry budget")
        self.attempts = attempts
        self.request_timeout = request_timeout
        self.backoff = backoff
        self.total_timeout = total_timeout
        self._slots = asyncio.Semaphore(5)
        self.clock = time.monotonic
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self._failures = 0
        self._opened_at = None
        self._probe_in_flight = False
        self._generation = 0

    async def get(self, client, url, headers=None):
        # Keep deadline ownership visible at admission's cancellation boundary.
        # Unlike wait_for's outer TimeoutError, this distinguishes our expiry
        # from caller cancellation while still including semaphore queue time.
        async with asyncio.timeout(self.total_timeout) as deadline:
            return await self._limited_get(client, url, headers, deadline)

    async def _limited_get(self, client, url, headers, deadline):
        # All state transitions have no await: atomic on this process's event loop.
        # Include queueing in the outer deadline; cancellation releases the slot.
        async with self._slots:
            probe = self._opened_at is not None
            if probe:
                if self.clock() - self._opened_at < self.cooldown or self._probe_in_flight:
                    raise RuntimeError("Gitea circuit open")
                self._probe_in_flight = True
            generation = self._generation
            try:
                result = await self._get(client, url, headers, 1 if probe else self.attempts)
            except asyncio.CancelledError:
                if generation == self._generation:
                    # Only an admitted operation's owned deadline is a failure.
                    # Queue expiry never reaches here; external cancel does not count.
                    if deadline.expired():
                        self._failures += 1
                    if probe or (deadline.expired() and self._failures >= self.failure_threshold):
                        self._trip()
                raise
            except (httpx.TransportError, httpx.HTTPStatusError, asyncio.TimeoutError) as error:
                if generation == self._generation:
                    transient = not isinstance(error, httpx.HTTPStatusError) or (
                        error.response.status_code == 429 or error.response.status_code >= 500
                    )
                    if transient:
                        self._failures += 1
                        if probe or self._failures >= self.failure_threshold:
                            self._trip()
                    else:
                        self._reset()
                raise
            except Exception:
                # Header factories and client validation can fail before dispatch.
                # Do not count these as transient outages, but never strand a probe.
                if probe and generation == self._generation:
                    self._trip()
                raise
            else:
                # A late success admitted before a trip must not close a newer circuit.
                if generation == self._generation:
                    self._reset()
                return result

    def _trip(self):
        self._opened_at = self.clock()
        self._probe_in_flight = False
        self._generation += 1

    def _reset(self):
        self._opened_at = None
        self._probe_in_flight = False
        self._failures = 0

    async def _get(self, client, url, headers, attempts):
        for attempt in range(attempts):
            try:
                attempt_headers = headers() if callable(headers) else headers
                response = await asyncio.wait_for(
                    client.get(url, headers=attempt_headers, timeout=self.request_timeout),
                    self.request_timeout,
                )
                response.raise_for_status()
                return response
            except (httpx.TransportError, httpx.HTTPStatusError, asyncio.TimeoutError) as error:
                if isinstance(error, httpx.HTTPStatusError):
                    status = error.response.status_code
                    if status != 429 and status < 500:
                        raise
                if attempt + 1 == attempts:
                    raise
                await asyncio.sleep(self.backoff * 2 ** attempt)


async def wait_for_task(client, url, headers, timeout):
    """Only a succeeded Meili task permits ACK; include HTTP waits in deadline."""
    async def poll():
        while True:
            response = await client.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            status = response.json()["status"]
            if status == "succeeded":
                return
            if status not in {"enqueued", "processing"}:
                raise RuntimeError(f"Meili task did not succeed: {status}")
            await asyncio.sleep(0.1)

    await asyncio.wait_for(poll(), timeout=timeout)
