"""Two bounded-domain repairs; real SDK/broker/ASGI except marked UNIT seams."""
import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import nats
import pytest
from nats.js.api import RawStreamMsg, ConsumerConfig, AckPolicy, PubAck
from nats.js.errors import NotFoundError

import newdlq
from test_bridge_runtime import bridge_loader, private_broker, until, hard_test_deadline, pytestmark


def strict_json(raw):
    def invalid(token):
        raise AssertionError('nonfinite JSON token: ' + token)
    return json.loads(raw, parse_constant=invalid)


def test_unit_existing_empty_sdk_record_requires_dlq_puback_before_ack():
    async def run():
        original = RawStreamMsg(subject='media.fix2', seq=1, data=None)
        msg = SimpleNamespace(data=b'{"stream":"EVENTS","consumer":"fix2","stream_seq":1}', ack=AsyncMock())
        js = SimpleNamespace(get_msg=AsyncMock(return_value=original), publish=AsyncMock(return_value=None))
        dlq = newdlq.DurableDLQ(js)
        # Direct call makes the SDK/JSON TypeError causal, not merely a timeout.
        with pytest.raises(RuntimeError, match='^DLQ PubAck unavailable$'):
            await dlq._persist(msg)
        msg.ack.assert_not_awaited()
        entry = strict_json(js.publish.await_args.args[1])
        assert entry['original_payload_b64'] == ''
        assert base64.b64decode(entry['original_payload_b64'], validate=True) == b''
        js.publish.return_value = PubAck(stream='BRIDGE_DLQ', seq=1)
        await dlq._persist(msg)
        msg.ack.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('missing', ['not-found', 'no-record', 'no-data-field'])
def test_unit_missing_original_never_becomes_empty_success(missing):
    async def run():
        fetch = AsyncMock(side_effect=NotFoundError()) if missing == 'not-found' else AsyncMock(
            return_value=None if missing == 'no-record' else SimpleNamespace(subject='media.fix2'))
        js = SimpleNamespace(get_msg=fetch, publish=AsyncMock())
        msg = SimpleNamespace(data=b'{"stream":"EVENTS","consumer":"fix2","stream_seq":1}', ack=AsyncMock())
        await newdlq.DurableDLQ(js).handle(msg)
        js.publish.assert_not_awaited()
        msg.ack.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['empty', 'headers-cap-empty'])
def test_real_generated_empty_advisory_puback_gate_and_http(bridge_loader, monkeypatch, kind):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            bridge = bridge_loader(broker.url)
            release, published = asyncio.Event(), asyncio.Event()
            try:
                await bridge.startup()
                js = nc.jetstream()
                kwargs = {}
                if kind == 'headers-cap-empty':
                    kwargs['headers'] = {'X': 'h' * (newdlq.SOURCE_MAX_MSG_BYTES - len(b'NATS/1.0\r\nX: \r\n\r\n'))}
                source = await js.publish('media.fix2', b'', **kwargs)
                sdk = await js.get_msg('EVENTS', seq=source.seq)
                assert sdk.data is None and sdk.sequence == source.seq and sdk.subject == 'media.fix2'
                wire = strict_json((await nc.request('$JS.API.STREAM.MSG.GET.EVENTS', json.dumps({'seq': source.seq}).encode(), timeout=1)).data)
                assert wire['message']['seq'] == source.seq
                assert base64.b64decode(wire['message'].get('data', ''), validate=True) == b''
                real_publish = bridge.dlq.js.publish
                async def gated_publish(*args, **options):
                    ack = await real_publish(*args, **options)
                    assert ack.stream == 'BRIDGE_DLQ' and type(ack.seq) is int and ack.seq > 0
                    published.set()
                    await release.wait()
                    return ack
                monkeypatch.setattr(bridge.dlq.js, 'publish', gated_publish)
                poison = await js.pull_subscribe('media.fix2', stream='EVENTS', durable='fix2-real',
                    config=ConsumerConfig(durable_name='fix2-real', ack_policy=AckPolicy.EXPLICIT, ack_wait=.05, max_deliver=1))
                assert (await poison.fetch(1, timeout=1))[0].data == b''
                await asyncio.sleep(.08)
                try:
                    await poison.fetch(1, timeout=.1)
                except (nats.errors.TimeoutError, asyncio.TimeoutError):
                    pass
                await asyncio.wait_for(published.wait(), timeout=2)
                pending = await js.consumer_info('BRIDGE_ADVISORIES', newdlq.DURABLE)
                assert pending.num_ack_pending == 1 and pending.ack_floor.stream_seq == 0
                assert (await js.stream_info('BRIDGE_DLQ')).state.messages == 1
                release.set()
                await until(lambda: js.consumer_info('BRIDGE_ADVISORIES', newdlq.DURABLE),
                    lambda c: c.num_ack_pending == 0 and c.ack_floor.stream_seq >= 1, 'empty real advisory ACK after PubAck')
                record = await js.get_msg('BRIDGE_DLQ', seq=1)
                entry = strict_json(record.data)
                adv = strict_json((await js.get_msg('BRIDGE_ADVISORIES', seq=1)).data)
                assert adv['deliveries'] == 1 and adv['consumer'] == 'fix2-real' and adv['stream_seq'] == source.seq
                assert entry['advisory'] == adv and entry['original_subject'] == 'media.fix2'
                assert base64.b64decode(entry['original_payload_b64'], validate=True) == b''
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app, raise_app_exceptions=False), base_url='http://bridge') as client:
                    assert (await client.get('/dlq')).status_code == 401
                    response = await client.get('/dlq', headers={'X-DLQ-Token': 'synthetic-dlq-token'})
                    assert response.status_code == 200
                    body = strict_json(response.content.decode('utf-8', errors='strict'))
                    assert body['items'][0] == entry and base64.b64decode(body['items'][0]['original_payload_b64'], validate=True) == b''
                print('FIX2_EMPTY ' + json.dumps(dict(kind=kind, sdk_data_is_none=True, original_api_exact=True,
                    generated_advisory=True, withheld_puback_unacked=True, authorized_http=response.status_code, exact_b64=True)))
            finally:
                release.set()
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['surrogate', 'nonfinite'])
@pytest.mark.parametrize('origin', ['generated', 'historical'])
def test_real_metadata_inspection_strict_http_lossless(bridge_loader, kind, origin):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False, connect_timeout=1)
            bridge = bridge_loader(broker.url)
            try:
                await bridge.startup()
                js = nc.jetstream()
                raw = (b'{"event_id":"\\ud800","request_id":"normal","trace_context":{}}'
                       if kind == 'surrogate' else
                       b'{"event_id":"normal","request_id":"normal","trace_context":{"value":1e999}}')
                raw += b' ' * (newdlq.SOURCE_MAX_MSG_BYTES - len(raw))
                assert len(raw) == newdlq.SOURCE_MAX_MSG_BYTES
                source = await js.publish('media.fix2.metadata', raw)
                assert (await js.get_msg('EVENTS', seq=source.seq)).data == raw
                if origin == 'generated':
                    poison = await js.pull_subscribe('media.fix2.metadata', stream='EVENTS', durable='fix2-meta',
                        config=ConsumerConfig(durable_name='fix2-meta', ack_policy=AckPolicy.EXPLICIT,
                                              ack_wait=.05, max_deliver=1))
                    assert (await poison.fetch(1, timeout=1))[0].data == raw
                    await asyncio.sleep(.08)
                    try:
                        await poison.fetch(1, timeout=.1)
                    except (nats.errors.TimeoutError, asyncio.TimeoutError):
                        pass
                    await until(lambda: js.consumer_info('BRIDGE_ADVISORIES', newdlq.DURABLE),
                        lambda c: c.num_ack_pending == 0 and c.ack_floor.stream_seq >= 1,
                        'metadata generated advisory persisted and ACKed')
                    advisory = strict_json((await js.get_msg('BRIDGE_ADVISORIES', seq=1)).data)
                    assert advisory['type'] == 'io.nats.jetstream.advisory.v1.max_deliver'
                    assert advisory['deliveries'] == 1 and advisory['consumer'] == 'fix2-meta'
                    assert advisory['stream_seq'] == source.seq
                    record = await js.get_msg('BRIDGE_DLQ', seq=1)
                    # Deliberately permissive here to reach the actual HTTP RED,
                    # even if the old writer persisted Python's Infinity token.
                    entry = json.loads(record.data)
                    assert entry['advisory'] == advisory
                else:
                    # Emulate an already persisted record from the old writer,
                    # independent of any future repair to the new-write path.
                    await bridge.shutdown()
                    envelope = json.loads(raw)
                    entry = dict(received_at='2026-01-01T00:00:00+00:00',
                        advisory={'stream': 'EVENTS', 'consumer': 'historical', 'stream_seq': source.seq},
                        original_subject='media.fix2.metadata',
                        original_payload_b64=base64.b64encode(raw).decode('ascii'),
                        event_id=envelope['event_id'], request_id=envelope['request_id'],
                        trace_context=envelope['trace_context'])
                    old_bytes = json.dumps(entry, separators=(',', ':')).encode('ascii')
                    historical_ack = await js.publish('bridge.dlq.events', old_bytes)
                    assert historical_ack.stream == 'BRIDGE_DLQ'
                    await bridge.startup()
                    record = await js.get_msg('BRIDGE_DLQ', seq=historical_ack.seq)
                    assert record.data == old_bytes
                assert len(record.data) <= newdlq.DLQ_JSON_MAX_BYTES
                assert base64.b64decode(entry['original_payload_b64'], validate=True) == raw
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=bridge.app,
                        raise_app_exceptions=False), base_url='http://bridge') as client:
                    assert (await client.get('/dlq')).status_code == 401
                    assert (await client.get('/readyz')).status_code == 200
                    response = await client.get('/dlq?limit=1', headers={'X-DLQ-Token': 'synthetic-dlq-token'})
                    assert response.status_code == 200, f'{origin}/{kind}: persisted exact source poisons HTTP inspection'
                    assert response.headers['content-type'].startswith('application/json')
                    body = strict_json(response.content.decode('utf-8', errors='strict'))
                    # Also reject escaped lone surrogates that a permissive parser
                    # accepts but cannot represent as Unicode scalar UTF-8.
                    json.dumps(body, ensure_ascii=False, allow_nan=False).encode('utf-8', errors='strict')
                    assert body['total'] == 1 and len(body['items']) == 1
                    item = body['items'][0]
                    assert base64.b64decode(item['original_payload_b64'], validate=True) == raw
                    assert item['original_subject'] == 'media.fix2.metadata'
                    assert item['advisory'] == entry['advisory']
                    assert item['metadata_unavailable'] is True
                    assert item['event_id'] is None and item['request_id'] is None and item['trace_context'] == {}
                # Read-time fallback must not rewrite historical broker records.
                assert (await js.get_msg('BRIDGE_DLQ', seq=record.sequence)).data == record.data
            finally:
                await bridge.shutdown()
                await nc.close()
    asyncio.run(run())
