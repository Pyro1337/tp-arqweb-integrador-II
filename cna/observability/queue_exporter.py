"""Read-only broker metadata. Never read payloads or create queues/resources."""
import asyncio
import os

import nats
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST

CONSUMERS = ("search-svc", "audit-svc-gitea-push", "audit-svc-gitea-issue-created",
             "audit-svc-gitea-issue-closed", "audit-svc-gitea-pull_request-merged")
app = FastAPI(title="queue-metrics")


async def observe_nats(js):
    values = {}
    for consumer in CONSUMERS:
        info = await js.consumer_info("EVENTS", consumer)
        values["nats:" + consumer] = {"waiting": info.num_pending, "inflight": info.num_ack_pending}
    info = await js.stream_info("BRIDGE_DLQ")
    values["nats:DLQ"] = {"stored": info.state.messages}
    return values


async def observe_redis(client):
    groups = await client.xinfo_groups("exports:jobs")
    group = next((g for g in groups if g["name"] == "export-workers"), None)
    if group is None or group.get("lag") is None:
        raise ValueError("queue metadata unavailable")
    return {"redis:export-workers": {"waiting": group["lag"], "inflight": group["pending"]}}


async def nats_snapshot():
    nc = await nats.connect(os.getenv("NATS_URL", "nats://nats:4222"),
                            connect_timeout=1, max_reconnect_attempts=0,
                            allow_reconnect=False)
    try:
        return await observe_nats(nc.jetstream(timeout=1))
    finally:
        await nc.close()


async def redis_snapshot():
    client = redis.from_url(os.getenv("REDIS_URL", "redis://export-redis:6379/0"),
                            decode_responses=True, socket_connect_timeout=1, socket_timeout=1)
    try:
        return await observe_redis(client)
    finally:
        await client.aclose()


async def render_metrics(nats_reader=nats_snapshot, redis_reader=redis_snapshot):
    registry = CollectorRegistry()
    up = Gauge("queue_collector_up", "Broker metadata observation succeeded", ["broker"], registry=registry)
    gauges = {name: Gauge("queue_" + name + "_messages", description, ["queue"], registry=registry)
              for name, description in (("waiting", "Not yet delivered to this consumer group"),
                                        ("inflight", "Delivered but not acknowledged"),
                                        ("stored", "Records retained in the DLQ stream"))}

    async def collect(broker, reader):
        try:
            values = await asyncio.wait_for(reader(), timeout=3)
            # Validate all observations before emitting any, so a failed scrape
            # cannot publish a partially plausible backlog for that broker.
            for fields in values.values():
                for name, count in fields.items():
                    if name not in gauges or type(count) is not int or count < 0:
                        raise ValueError("invalid queue count")
            for queue, fields in values.items():
                for name, count in fields.items():
                    gauges[name].labels(queue=queue).set(count)
            up.labels(broker=broker).set(1)
        except Exception:
            up.labels(broker=broker).set(0)
    await asyncio.gather(collect("nats", nats_reader), collect("redis", redis_reader))
    return generate_latest(registry)


@app.get("/metrics")
async def metrics():
    return Response(await render_metrics(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
