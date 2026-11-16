"""UNIT/ASGI wiring tests; fake broker owns persistent resources."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import app
from newdlq import validate_payload_budget
from test_dlq import UnitResources, advisory


def connection(broker):
    return SimpleNamespace(jetstream=lambda: broker, is_connected=True, max_payload=1024 * 1024,
                           subscribe=AsyncMock(), close=AsyncMock())


@pytest.fixture(autouse=True)
def unit_transport_boundary(monkeypatch):
    # Real payload validation; only the disposable protocol connection is fake.
    monkeypatch.setattr(app, 'validate_transport', AsyncMock(side_effect=validate_payload_budget))


def test_app_startup_binds_durable_capture_and_shutdown_keeps_resources(monkeypatch):
    async def run():
        broker = UnitResources()
        nc = connection(broker)
        monkeypatch.setattr(app.nats, 'connect', AsyncMock(return_value=nc))
        try:
            await app.startup()
            assert set(broker.streams) == {'EVENTS', 'BRIDGE_DLQ', 'BRIDGE_ADVISORIES'}
            nc.subscribe.assert_not_awaited()
            await broker.subscribe_options['cb'](advisory(broker))
            assert broker.order == ['fetch', 'puback', 'ack']
            assert len(broker.records) == 1
            assert app.dlq.ready is True
        finally:
            if hasattr(app, 'shutdown'):
                await app.shutdown()
        nc.close.assert_awaited_once()
        assert len(broker.streams) == 3 and broker.consumer is not None
        assert not app.dlq.ready
    asyncio.run(run())


def test_app_startup_resource_failure_closes_client_safely(monkeypatch, caplog):
    async def run():
        broker = UnitResources()
        broker.stream_info = AsyncMock(side_effect=RuntimeError('nats://credential@private'))
        nc = connection(broker)
        monkeypatch.setattr(app.nats, 'connect', AsyncMock(return_value=nc))
        with pytest.raises(RuntimeError, match='^broker resources unavailable$'):
            await app.startup()
        nc.close.assert_awaited_once()
        assert not app.dlq.ready
        assert 'credential' not in caplog.text
    asyncio.run(run())


def test_readyz_requires_actual_capture_not_connected_only(monkeypatch):
    async def run():
        monkeypatch.setattr(app, 'nc', connection(UnitResources()))
        monkeypatch.setattr(app, 'dlq', None, raising=False)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            response = await client.get('/readyz')
        assert response.status_code == 503
        assert response.json() == {'detail': 'broker resources unavailable'}
    asyncio.run(run())
