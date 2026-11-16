"""Fix-cycle regressions. Private real NATS; UNIT boundaries explicitly identified."""
import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import nats
import pytest
from nats.js.api import SubjectTransform, StreamSource, RePublish, PubAck
from nats.js.errors import NotFoundError

import app
import newdlq
from test_bridge_runtime import private_broker, bridge_loader, until, hard_test_deadline, pytestmark
from test_dlq import UnitResources


@pytest.mark.parametrize('field,value', [
    ('subject_transform', SubjectTransform(src='gitea.>', dest='hidden.>')),
    ('mirror', StreamSource(name='OTHER')),
    ('sources', [StreamSource(name='OTHER')]),
    ('republish', RePublish(src='>', dest='hidden.>', headers_only=True)),
])
def test_unit_semantic_stream_drift_rejected_before_any_mutation(field, value):
    async def run():
        broker = UnitResources()
        configs = newdlq.DurableDLQ.stream_configs()
        # A missing first stream must not be created before a later conflict is found.
        broker.streams[configs[-1].name] = configs[-1]
        setattr(configs[-1], field, value)
        broker.add_stream = AsyncMock(wraps=broker.add_stream)
        broker.subscribe = AsyncMock(wraps=broker.subscribe)
        helper = newdlq.DurableDLQ(broker)
        with pytest.raises(newdlq.ResourceConflict):
            await helper.setup()
        broker.add_stream.assert_not_awaited()
        broker.subscribe.assert_not_awaited()
        assert not helper.ready and helper.subscription is None
    asyncio.run(run())


def test_real_advisory_transform_rejects_before_binding_preserves_data(bridge_loader):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                js = nc.jetstream()
                for config in newdlq.DurableDLQ.stream_configs():
                    if config.name == 'BRIDGE_ADVISORIES':
                        config.subject_transform = SubjectTransform(src=newdlq.ADVISORY_SUBJECT, dest='hidden.>')
                    await js.add_stream(config=config)
                original = bytes([0, 255, 1])
                src = await js.publish('gitea.push', original)
                adv = await js.publish('$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.EVENTS.fix1',
                    json.dumps({'stream': 'EVENTS', 'consumer': 'fix1', 'stream_seq': src.seq}).encode())
                before = {c.name: await js.stream_info(c.name) for c in newdlq.DurableDLQ.stream_configs()}
                retained = await js.get_msg('BRIDGE_ADVISORIES', seq=adv.seq)
                assert retained.subject == 'hidden.EVENTS.fix1'
                with pytest.raises(RuntimeError, match='broker resources unavailable'):
                    await bridge.startup()
                assert not bridge.dlq.ready and bridge.dlq.subscription is None
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    assert (await client.get('/readyz')).status_code == 503
                for name, info in before.items():
                    after = await js.stream_info(name)
                    assert after.config == info.config and after.state == info.state
                with pytest.raises(NotFoundError):
                    await js.consumer_info('BRIDGE_ADVISORIES', newdlq.DURABLE)
                assert (await js.get_msg('EVENTS', seq=src.seq)).data == original
                after = await js.get_msg('BRIDGE_ADVISORIES', seq=adv.seq)
                assert after.data == retained.data and after.subject == retained.subject
            finally:
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize('boundary', ['ingress', 'dlq'])
@pytest.mark.parametrize('seq', [True, False, 0, -1, 1.5, '1', None])
def test_unit_sdk_puback_requires_exact_positive_integer_ingress_and_dlq(monkeypatch, seq, boundary):
    async def run():
        ack = PubAck.from_response({'stream': 'EVENTS', 'seq': seq})
        assert ack.seq is seq  # SDK does not normalize the malformed response.
        if boundary == 'ingress':
            monkeypatch.setattr(app, 'js', SimpleNamespace(publish=AsyncMock(return_value=ack)))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
                response = await client.post('/webhook/test-secret', json={}, headers={'X-Gitea-Event': 'push'})
            assert response.status_code == 503
        else:
            msg = SimpleNamespace(data=b'{"stream":"EVENTS","consumer":"fix1","stream_seq":1}', ack=AsyncMock())
            js = SimpleNamespace(get_msg=AsyncMock(return_value=SimpleNamespace(data=b'{}', subject='gitea.push')),
                                 publish=AsyncMock(return_value=PubAck.from_response({'stream': 'BRIDGE_DLQ', 'seq': seq})))
            await newdlq.DurableDLQ(js).handle(msg)
            msg.ack.assert_not_awaited()
    asyncio.run(run())


def test_real_oversized_http_and_other_producer_rejected_before_storage(bridge_loader):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                await bridge.startup()
                js = nc.jetstream()
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    response = await client.post('/webhook/private-integration-secret',
                        json={'action': 'opened', 'issue': {'title': 'x' * 50000}},
                        headers={'X-Gitea-Event': 'issues'})
                assert response.status_code == 413
                with pytest.raises(nats.js.errors.APIError):
                    await js.publish('media.from.other.producer', b'x' * (16 * 1024 + 1))
                assert (await js.stream_info('EVENTS')).state.messages == 0
            finally:
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize('profile', ['payload-too-small', 'control-line-too-large'])
def test_real_transport_contract_rejects_before_any_resource_creation(bridge_loader, profile):
    from broker_fixtures import NatsBroker
    async def run():
        broker = NatsBroker()
        nc = None
        bridge = bridge_loader('unused')
        try:
            # Explicit negative profiles stay negative when the default fixture grows.
            payload = 65536 if profile == 'payload-too-small' else 1048576
            broker.config_text = broker.config_text.replace('max_payload: 64KB', f'max_payload: {payload}').replace('max_payload: 512KB', f'max_payload: {payload}')
            if profile == 'control-line-too-large':
                broker.config_text += '\nmax_control_line: 8192\n'
            broker.config.write_text(broker.config_text)
            await asyncio.to_thread(broker.__enter__)
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            with pytest.raises(RuntimeError, match='broker resources unavailable'):
                await bridge.startup()
            assert not bridge.dlq.ready and bridge.dlq.subscription is None
            assert (await nc.jetstream().account_info()).streams == 0
        finally:
            await bridge.shutdown()
            if nc is not None:
                await nc.close()
            await asyncio.to_thread(broker.close)
            assert broker.process.poll() is not None and not broker.root.exists()
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['binary', 'metadata-worst-expansion', 'deep-metadata'])
def test_real_source_at_cap_and_advisory_at_cap_deadletter_losslessly(bridge_loader, kind):
    def compact(value):
        return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()

    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                await bridge.startup()
                js = nc.jetstream()
                limit = newdlq.SOURCE_MAX_MSG_BYTES
                subject = 'media.boundary'
                metadata = None
                if kind == 'binary':
                    original = (bytes([0, 255, 1]) * ((limit + 2) // 3))[:limit]
                elif kind == 'deep-metadata':
                    original = b'{"trace_context":' + b'[' * 1500 + b'0' + b']' * 1500 + b'}'
                    original += b' ' * (limit - len(original))
                else:
                    # DEL is legal unescaped JSON: each raw byte becomes six bytes
                    # in ensure_ascii output. All metadata subtrees are projected.
                    metadata = {'event_id': chr(127) * 5000, 'request_id': chr(127) * 5000,
                                'trace_context': {'raw': ''}}
                    metadata['trace_context']['raw'] = chr(127) * (limit - len(compact(metadata)))
                    original = compact(metadata)
                    # Close to the actual control-line subject limit including
                    # the SDK request inbox and decimal payload length.
                    subject = 'media.' + chr(127) * (newdlq.SUBJECT_MAX_BYTES - 128)
                assert len(original) == limit
                source = await js.publish(subject, original)
                advisory = {'stream': 'EVENTS', 'consumer': 'c' * 255, 'stream_seq': source.seq, 'extra': ''}
                advisory['extra'] = chr(127) * (newdlq.ADVISORY_MAX_MSG_BYTES - len(compact(advisory)))
                raw = compact(advisory)
                assert len(raw) == newdlq.ADVISORY_MAX_MSG_BYTES
                adv = await js.publish('$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.EVENTS.boundary', raw)
                await until(lambda: js.consumer_info('BRIDGE_ADVISORIES', newdlq.DURABLE),
                            lambda c: c.ack_floor.stream_seq >= adv.seq and c.num_ack_pending == 0,
                            'near-bound advisory ACK after real DLQ PubAck')
                stored = await js.get_msg('BRIDGE_DLQ', seq=1)
                entry = json.loads(stored.data)
                assert base64.b64decode(entry['original_payload_b64']) == original
                assert entry['original_subject'] == subject and entry['advisory'] == advisory
                if metadata is not None:
                    for key in metadata:
                        assert entry[key] == metadata[key]
                    assert len(stored.data) > 160000, len(stored.data)
                assert len(stored.data) <= newdlq.DLQ_JSON_MAX_BYTES
                assert (await js.get_msg('EVENTS', seq=source.seq)).data == original
                for stream_subject, oversize in [(subject, original + b'x'),
                    ('$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.EVENTS.boundary', raw + b' '),
                    ('media.header', b'x' * limit)]:
                    kwargs = {'headers': {'Extra': 'header-counts-too'}} if stream_subject == 'media.header' else {}
                    with pytest.raises(nats.js.errors.APIError):
                        await js.publish(stream_subject, oversize, **kwargs)
                assert (await js.stream_info('EVENTS')).state.messages == 1
                assert (await js.stream_info('BRIDGE_ADVISORIES')).state.messages == 1
                assert (await js.stream_info('BRIDGE_DLQ')).state.messages == 1
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    assert (await client.get('/readyz')).status_code == 200
                print('PAYLOAD_BOUNDARY ' + json.dumps({'kind': kind, 'source_bytes': len(original),
                      'advisory_bytes': len(raw), 'subject_bytes': len(subject.encode()),
                      'dlq_json_bytes': len(stored.data), 'dlq_cap': newdlq.DLQ_MAX_MSG_BYTES,
                      'broker_max_payload': broker.protocol_info['max_payload']}))
            finally:
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())


def test_real_exact_http_boundary_then_actual_max_delivery_advisory(bridge_loader):
    from nats.js.api import ConsumerConfig, AckPolicy, DeliverPolicy
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                await bridge.startup()
                js = nc.jetstream()
                headers = {'X-Gitea-Event': 'issues', 'X-Gitea-Delivery': 'boundary-http', 'X-Request-ID': 'boundary-http'}
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app), base_url='http://bridge') as client:
                    body = {'action': 'opened', 'issue': {'title': ''}}
                    assert (await client.post('/webhook/private-integration-secret', json=body, headers=headers)).status_code == 200
                    sample = await js.get_msg('EVENTS', seq=1)
                    body['issue']['title'] = 'x' * (newdlq.SOURCE_MAX_MSG_BYTES - len(sample.data))
                    response = await client.post('/webhook/private-integration-secret', json=body, headers=headers)
                    assert response.status_code == 200
                    source = await js.get_msg('EVENTS', seq=2)
                    assert len(source.data) == newdlq.SOURCE_MAX_MSG_BYTES
                    body['issue']['title'] += 'x'
                    rejected = await client.post('/webhook/private-integration-secret', json=body, headers=headers)
                    assert rejected.status_code == 413 and rejected.json() == {'detail': 'event envelope too large'}
                    assert (await js.stream_info('EVENTS')).state.messages == 2
                poison = await js.pull_subscribe('gitea.issue.created', stream='EVENTS', durable='boundary-poison',
                    config=ConsumerConfig(durable_name='boundary-poison', ack_policy=AckPolicy.EXPLICIT,
                        ack_wait=0.05, max_deliver=1, deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=2))
                assert (await poison.fetch(1, timeout=1))[0].data == source.data
                await asyncio.sleep(0.08)
                try:
                    await poison.fetch(1, timeout=0.1)
                except (nats.errors.TimeoutError, asyncio.TimeoutError):
                    pass
                await until(lambda: js.stream_info('BRIDGE_DLQ'), lambda i: i.state.messages == 1, 'actual maxdelivery at source cap')
                stored = json.loads((await js.get_msg('BRIDGE_DLQ', seq=1)).data)
                assert base64.b64decode(stored['original_payload_b64']) == source.data
                assert stored['advisory']['consumer'] == 'boundary-poison'
                assert stored['advisory']['deliveries'] == 1 and stored['advisory']['stream_seq'] == 2
                await until(lambda: js.consumer_info('BRIDGE_ADVISORIES', newdlq.DURABLE),
                            lambda c: c.num_ack_pending == 0 and c.ack_floor.stream_seq >= 1, 'actual advisory ACK')
            finally:
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize('stream', ['EVENTS', 'BRIDGE_ADVISORIES', 'BRIDGE_DLQ'])
def test_real_unlimited_legacy_message_cap_rejected_preserving_every_byte(bridge_loader, stream):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            try:
                js = nc.jetstream()
                config = next(c for c in newdlq.DurableDLQ.stream_configs() if c.name == stream)
                config.max_msg_size = -1
                await js.add_stream(config=config)
                subject = {'EVENTS': 'gitea.push', 'BRIDGE_ADVISORIES': '$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.EVENTS.old',
                           'BRIDGE_DLQ': 'bridge.dlq.events'}[stream]
                original = bytes([0, 255, 1])
                ack = await js.publish(subject, original)
                before = await js.stream_info(stream)
                with pytest.raises(RuntimeError, match='broker resources unavailable'):
                    await bridge.startup()
                after = await js.stream_info(stream)
                assert after.config == before.config and after.state == before.state
                assert (await js.get_msg(stream, seq=ack.seq)).data == original
                assert (await js.account_info()).streams == 1
                assert not bridge.dlq.ready and bridge.dlq.subscription is None
            finally:
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize('outcome', [
    'expected-error', 'unrelated-error', 'misleading-error', 'disconnect',
    'network-error', 'no-error',
])
def test_unit_transport_waits_for_identified_protocol_error_after_flush(monkeypatch, outcome):
    """UNIT: control the wire timing, but exercise the real probe error handler.

    A PONG is not the oversized SUB rejection. Deliver its parsed -ERR only on
    a later event-loop turn; close always cancels/joins the synthetic wire task.
    """
    async def run():
        order = []
        probes = []
        base = newdlq._SubjectBudgetProbe

        class ControlledProbe(base):
            def __init__(self):
                super().__init__()
                self.delivery = None
                self.closed = False
                probes.append(self)

            async def connect(self, url, **options):
                assert url == 'nats://owned-unit-probe:4222'
                assert options['allow_reconnect'] is False
                self._error_cb = options['error_cb']

            async def subscribe(self, subject):
                assert len(subject.encode()) == newdlq.SUBJECT_MAX_BYTES - 1

            async def flush(self, timeout):
                order.append('flush-pong')

                async def deliver_later():
                    await asyncio.sleep(0)
                    if outcome in ('expected-error', 'unrelated-error', 'misleading-error'):
                        message = {
                            'expected-error': "'Maximum Control Line Exceeded'",
                            'unrelated-error': "'Authorization Violation'",
                            'misleading-error': "'Not Maximum Control Line Exceeded'",
                        }[outcome]
                        order.append('protocol-error')
                        await self._process_err(message)
                    elif outcome in ('disconnect', 'network-error'):
                        self.closed = True  # Disconnect without parsed -ERR proves nothing.
                        order.append('disconnect')
                self.delivery = asyncio.create_task(deliver_later())
                if outcome == 'network-error':
                    raise nats.errors.Error('synthetic network failure')

            async def _close(self, *args):
                self.closed = True  # No real socket exists in this UNIT boundary.

            async def close(self):
                if self.delivery is not None:
                    if not self.delivery.done():
                        self.delivery.cancel()
                    await asyncio.gather(self.delivery, return_exceptions=True)
                    if not self.delivery.cancelled():
                        self.delivery.result()
                self.closed = True
                order.append('close')

        monkeypatch.setattr(newdlq, '_SubjectBudgetProbe', ControlledProbe)
        nc = SimpleNamespace(max_payload=newdlq.BROKER_MIN_MAX_PAYLOAD,
            connected_url=SimpleNamespace(geturl=lambda: 'nats://owned-unit-probe:4222'))
        if outcome == 'expected-error':
            await newdlq.validate_transport(nc)
            assert order == ['flush-pong', 'protocol-error', 'close']
            assert probes[0].control_line_rejected is True
        else:
            with pytest.raises(newdlq.ResourceConflict, match='incompatible broker subject budget'):
                await newdlq.validate_transport(nc)
        assert len(probes) == 1 and probes[0].closed
        assert probes[0].delivery.done()
    asyncio.run(run())


def test_unit_http_oversize_does_not_attempt_publish(monkeypatch):
    async def run():
        publish = AsyncMock()
        monkeypatch.setattr(app, 'js', SimpleNamespace(publish=publish))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://bridge') as client:
            response = await client.post('/webhook/test-secret', json={'action': 'opened', 'issue': {'title': 'x' * 50000}},
                                         headers={'X-Gitea-Event': 'issues'})
        assert response.status_code == 413
        publish.assert_not_awaited()
    asyncio.run(run())
