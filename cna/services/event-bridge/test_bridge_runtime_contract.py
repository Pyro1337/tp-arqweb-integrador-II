"""Real stream conflicts reject without mutating existing data or configuration."""
import asyncio
import json
import nats
import pytest
import newdlq
from nats.js.api import StreamConfig
from test_bridge_runtime import private_broker, pytestmark, hard_test_deadline


@pytest.mark.parametrize("change", ["subjects", "max_bytes", "max_msgs_per_subject", "discard", "max_msg_size", "discard_new_per_subject"])
def test_real_incompatible_stream_fails_closed_preserving_messages(change):
    async def run():
        async with asyncio.timeout(15), private_broker() as broker:
            nc = await nats.connect(broker.url, allow_reconnect=False)
            try:
                js = nc.jetstream()
                # Start from the complete bounded contract so each individual
                # negative still tests its named drift, not an unrelated size cap.
                config = newdlq.DurableDLQ.stream_configs()[0]
                if change == "subjects":
                    config.subjects = ["gitea.>", "export.*", "media.*"]
                elif change == "max_bytes":
                    config.max_bytes = 8 * 1024**2
                elif change == "max_msgs_per_subject":
                    config.max_msgs_per_subject = 1
                elif change == "discard":
                    config.discard = "new"
                elif change == "max_msg_size":
                    config.max_msg_size = 1024
                else:
                    config.discard = "new"
                    config.max_msgs_per_subject = 1
                    config.discard_new_per_subject = True
                if change == "discard_new_per_subject":
                    # Installed nats-py omits this server field: use the real API
                    # rather than silently assigning an unserialized SDK attribute.
                    raw = config.as_dict()
                    raw["discard_new_per_subject"] = True
                    reply = await nc.request('$JS.API.STREAM.CREATE.EVENTS', json.dumps(raw).encode())
                    result = json.loads(reply.data)
                    assert "error" not in result, result
                    assert result["config"]["discard_new_per_subject"] is True
                else:
                    await js.add_stream(config=config)
                ack = await js.publish("gitea.push", b'keep-me')
                before = await js.stream_info("EVENTS")
                dlq_module = newdlq
                dlq = dlq_module.DurableDLQ(js)
                with pytest.raises(dlq_module.ResourceConflict, match="incompatible"):
                    await dlq.setup()
                assert dlq.ready is False
                after = await js.stream_info("EVENTS")
                assert after.config == before.config
                assert after.state.first_seq == before.state.first_seq
                assert after.state.last_seq == before.state.last_seq
                assert after.state.messages == before.state.messages == 1
                assert (await js.get_msg("EVENTS", seq=ack.seq)).data == b'keep-me'
                print("BOOTSTRAP_CONFLICT_PRESERVED " + change)
            finally:
                await nc.close()
    asyncio.run(run())
