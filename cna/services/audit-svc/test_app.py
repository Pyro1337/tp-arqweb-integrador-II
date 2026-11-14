import json
import uuid
from unittest.mock import AsyncMock
import pytest
import app as app_module


@pytest.mark.asyncio
async def test_idempotencia_descarta_evento_duplicado():
    class FakeConn:
        def __init__(self):
            self.calls = 0

        async def fetchval(self, query, *args):
            self.calls += 1
            return None if self.calls > 1 else args[0]

    class FakePool:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *a):
            pass

    pool = FakePool()
    pool.conn = FakeConn()
    app_module.pool = pool

    event_id = str(uuid.uuid4())
    envelope = {
        "event_id": event_id,
        "type": "gitea.push",
        "occurred_at": "2026-11-12T20:00:00+00:00",
        "request_id": "req-1",
        "payload": {"repo": "equipo/repo", "pusher": "arqweb2026"},
    }

    class FakeMsg:
        def __init__(self, data):
            self.data = data
            self.acked = False

        async def ack(self):
            self.acked = True

    msg1 = FakeMsg(json.dumps(envelope).encode())
    msg2 = FakeMsg(json.dumps(envelope).encode())

    await app_module.handle_event(msg1)
    await app_module.handle_event(msg2)

    assert msg1.acked and msg2.acked
    assert pool.conn.calls == 2  # se intento dos veces, pero el insert solo pego una


def test_healthz_no_depende_de_la_db():
    import asyncio
    result = asyncio.run(app_module.healthz())
    assert result == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_503_si_nats_no_esta_listo():
    app_module.pool = AsyncMock()
    app_module.ready["db"] = True
    app_module.ready["nats"] = False
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        await app_module.readyz()
    assert exc_info.value.status_code == 503
    app_module.ready["nats"] = True
