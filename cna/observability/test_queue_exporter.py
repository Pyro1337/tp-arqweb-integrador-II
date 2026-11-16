from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from prometheus_client.parser import text_string_to_metric_families
import queue_exporter as module


@pytest.mark.asyncio
async def test_nats_depth_uses_broker_pending_not_stored_messages():
    js = AsyncMock()
    js.consumer_info.return_value = SimpleNamespace(num_pending=7, num_ack_pending=3)
    js.stream_info.return_value = SimpleNamespace(state=SimpleNamespace(messages=2))
    result = await module.observe_nats(js)
    assert result["nats:search-svc"] == {"waiting": 7, "inflight": 3}
    assert result["nats:DLQ"] == {"stored": 2}
    js.get_msg.assert_not_awaited()
    js.stream_info.assert_awaited_once_with("BRIDGE_DLQ")


@pytest.mark.asyncio
async def test_redis_depth_uses_group_lag_and_pending_not_stream_length():
    client = AsyncMock()
    client.xinfo_groups.return_value = [{"name": "export-workers", "lag": 9, "pending": 4}]
    assert await module.observe_redis(client) == {"redis:export-workers": {"waiting": 9, "inflight": 4}}
    client.xlen.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_redis_lag_does_not_become_zero():
    client = AsyncMock()
    client.xinfo_groups.return_value = [{"name": "export-workers", "lag": None, "pending": 4}]
    with pytest.raises(ValueError):
        await module.observe_redis(client)


@pytest.mark.asyncio
async def test_collector_error_omits_queue_values_and_reports_down():
    async def failed(): raise RuntimeError("sensitive-connection-string")
    async def healthy(): return {"redis:export-workers": {"waiting": 3, "inflight": 1}}
    text = (await module.render_metrics(failed, healthy)).decode()
    assert 'queue_collector_up{broker="nats"} 0.0' in text
    assert 'queue_collector_up{broker="redis"} 1.0' in text
    assert 'queue="nats:search-svc"' not in text
    assert 'sensitive-connection-string' not in text
    assert 'queue_waiting_messages{queue="redis:export-workers"} 3.0' in text
    assert list(text_string_to_metric_families(text))
