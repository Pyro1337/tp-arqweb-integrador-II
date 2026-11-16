"""Focused review regressions; private real NATS, no shared service mutations."""
import asyncio
import base64
import json

import httpx
import nats
import pytest

from test_bridge_runtime import bridge_loader, private_broker, startup, until, pytestmark, hard_test_deadline


def test_real_headers_only_drift_preserves_durable_and_messages(bridge_loader):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                js = nc.jetstream()
                helper = bridge.DurableDLQ(js)
                for config in helper.stream_configs():
                    await js.add_stream(config=config)
                config = helper.consumer_config()
                config.headers_only = True
                config.deliver_subject = 'private.review.delivery'
                await js.add_consumer('BRIDGE_ADVISORIES', config=config)
                source = await js.publish('gitea.push', b'keep-source')
                adv = await js.publish('$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.EVENTS.x', b'keep-advisory')
                before = await js.consumer_info('BRIDGE_ADVISORIES', 'bridge-dlq-v1')
                with pytest.raises(RuntimeError, match='incompatible'):
                    await helper.setup()
                assert not helper.ready and helper.subscription is None
                after = await js.consumer_info('BRIDGE_ADVISORIES', 'bridge-dlq-v1')
                assert after.config == before.config
                assert after.delivered == before.delivered
                assert (await js.get_msg('EVENTS', seq=source.seq)).data == b'keep-source'
                assert (await js.get_msg('BRIDGE_ADVISORIES', seq=adv.seq)).data == b'keep-advisory'
                assert (await js.stream_info('BRIDGE_DLQ')).state.messages == 0
            finally:
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize("detach", ["wrapper", "underlying"])
def test_real_detached_subscription_is_not_ready_resources_intact(bridge_loader, detach):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                await startup(bridge, nc)
                js = nc.jetstream()
                source = await js.publish('gitea.push', b'keep-source')
                configs = {name: (await js.stream_info(name)).config for name in
                           ('EVENTS', 'BRIDGE_ADVISORIES', 'BRIDGE_DLQ')}
                consumer = (await js.consumer_info('BRIDGE_ADVISORIES', 'bridge-dlq-v1')).config
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    assert (await client.get('/readyz')).status_code == 200
                    sub = bridge.dlq.subscription
                    await (sub if detach == "wrapper" else sub._sub).unsubscribe()
                    response = await client.get('/readyz')
                    assert response.status_code == 503
                    assert response.json() == {'detail': 'broker resources unavailable'}
                for name, config in configs.items():
                    assert (await js.stream_info(name)).config == config
                assert (await js.consumer_info('BRIDGE_ADVISORIES', 'bridge-dlq-v1')).config == consumer
                assert (await js.get_msg('EVENTS', seq=source.seq)).data == b'keep-source'
            finally:
                await bridge.app.router.shutdown()
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize("state", ["none-wrapper", "none-underlying", "missing-wrapper", "missing-underlying",
                                 "raises-wrapper", "raises-underlying"])
def test_real_unknown_liveness_after_underlying_unsubscribe_is_not_ready(bridge_loader, state, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    class Unreadable:
        @property
        def _closed(self):
            raise RuntimeError("private liveness detail must not escape")

    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            real_sub = None
            try:
                await startup(bridge, nc)
                js = nc.jetstream()
                original = b'\x00keep-original\xff'
                source = await js.publish('gitea.push', original)
                configs = {name: (await js.stream_info(name)).config for name in
                           ('EVENTS', 'BRIDGE_ADVISORIES', 'BRIDGE_DLQ')}
                consumer = (await js.consumer_info('BRIDGE_ADVISORIES', 'bridge-dlq-v1')).config
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    assert (await client.get('/readyz')).status_code == 200
                    real_sub = bridge.dlq.subscription
                    assert real_sub._closed is False and real_sub._sub._closed is False
                    await real_sub._sub.unsubscribe()
                    assert real_sub._sub._closed is True
                    assert (await client.get('/readyz')).status_code == 503
                    candidate = (SimpleNamespace(_closed=None) if state.startswith('none') else
                                 Unreadable() if state.startswith('raises') else SimpleNamespace())
                    # Match the independent original probe, after real detachment.
                    bridge.dlq.subscription = (SimpleNamespace(_closed=False, _sub=candidate)
                                               if state.endswith('underlying') else candidate)
                    stream_reads = AsyncMock(wraps=bridge.dlq.js.stream_info)
                    consumer_reads = AsyncMock(wraps=bridge.dlq.js.consumer_info)
                    with monkeypatch.context() as patch:
                        patch.setattr(bridge.dlq.js, 'stream_info', stream_reads)
                        patch.setattr(bridge.dlq.js, 'consumer_info', consumer_reads)
                        response = await client.get('/readyz')
                    print('L4_REAL_STATUS ' + json.dumps({'state': state, 'status': response.status_code,
                                                         'actual_underlying_closed': real_sub._sub._closed}))
                    assert response.status_code == 503
                    assert response.json() == {'detail': 'broker resources unavailable'}
                    stream_reads.assert_not_awaited()
                    consumer_reads.assert_not_awaited()
                assert nc.is_connected and bridge.nc.is_connected
                for name, config in configs.items():
                    assert (await js.stream_info(name)).config == config
                assert (await js.consumer_info('BRIDGE_ADVISORIES', 'bridge-dlq-v1')).config == consumer
                assert (await js.get_msg('EVENTS', seq=source.seq)).data == original
                assert (await js.stream_info('BRIDGE_DLQ')).state.messages == 0
            finally:
                if real_sub is not None:
                    bridge.dlq.subscription = real_sub
                await bridge.app.router.shutdown()
                await nc.close()
    asyncio.run(run())


def test_real_dlq_auth_nondisclosure_and_authorized_original_bytes(bridge_loader, monkeypatch, caplog):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                monkeypatch.delenv('DLQ_READ_TOKEN', raising=False)
                await startup(bridge, nc)
                js = nc.jetstream()
                original = b'{"event_id":"sensitive-event","request_id":"sensitive-request","trace_context":{"private":"sensitive-context"},"payload":{"secret":"sensitive-source"}}'
                source = await js.publish('gitea.push', original)
                await js.publish('$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.EVENTS.auth', json.dumps(
                    {'stream': 'EVENTS', 'consumer': 'auth-source', 'stream_seq': source.seq}).encode())
                await until(lambda: js.stream_info('BRIDGE_DLQ'), lambda i: i.state.messages == 1, 'protected source persisted')
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    assert (await client.get('/readyz')).status_code == 200
                    disabled = await client.get('/dlq', headers={'X-DLQ-Token': 'private-integration-secret'})
                    assert disabled.status_code == 503
                    assert disabled.json() == {'detail': 'DLQ inspection disabled'}
                    monkeypatch.setenv('DLQ_READ_TOKEN', 'synthetic-dlq-token')
                    for headers in ({}, {'X-DLQ-Token': 'wrong'}, {'X-DLQ-Token': 'private-integration-secret'}):
                        response = await client.get('/dlq', headers=headers)
                        assert response.status_code == 401
                        assert response.json() == {'detail': 'unauthorized'}
                        assert 'sensitive-' not in response.text + caplog.text
                    authorized = await client.get('/dlq', headers={'X-DLQ-Token': 'synthetic-dlq-token'})
                    assert authorized.status_code == 200
                    assert base64.b64decode(authorized.json()['items'][0]['original_payload_b64']) == original
                assert (await js.get_msg('EVENTS', seq=source.seq)).data == original
                assert (await js.stream_info('BRIDGE_DLQ')).state.messages == 1
            finally:
                await bridge.app.router.shutdown()
                await nc.close()
    asyncio.run(run())
