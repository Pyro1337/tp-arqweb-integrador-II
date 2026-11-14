import os
import json
import logging
import sys
import asyncio
from datetime import datetime, timezone

import asyncio
import httpx
import nats
from nats.js.api import ConsumerConfig
from fastapi import FastAPI, HTTPException

GITEA_URL = os.environ.get("GITEA_URL", "http://host.docker.internal:3000")
GITEA_TOKEN = os.environ.get("GITEA_TOKEN", "")
MEILI_URL = os.environ.get("MEILI_URL", "http://meilisearch:7700")
MEILI_KEY = os.environ.get("MEILI_KEY", "masterKey123")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
INDEX = "gitea_content"

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("search-svc")

app = FastAPI(title="search-svc")
nc = None
ready = {"meili": False, "nats": False}
seen_events = set()


def jlog(**kwargs):
    kwargs.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    kwargs.setdefault("service", "search-svc")
    log.info(json.dumps(kwargs))


async def meili_headers():
    return {"Authorization": f"Bearer {MEILI_KEY}", "Content-Type": "application/json"}


async def ensure_index():
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{MEILI_URL}/indexes", json={"uid": INDEX, "primaryKey": "id"},
                               headers=await meili_headers(), timeout=5)
            ready["meili"] = True
        except Exception as e:
            jlog(level="error", message=f"no se pudo crear indice: {e}")


async def handle_push(msg):
    try:
        envelope = json.loads(msg.data.decode())
        event_id = envelope["event_id"]
        if event_id in seen_events:
            await msg.ack()
            return
        seen_events.add(event_id)

        repo = envelope["payload"]["repo"]
        request_id = envelope.get("request_id")

        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"token {GITEA_TOKEN}"} if GITEA_TOKEN else {}
            resp = await client.get(f"{GITEA_URL}/api/v1/repos/{repo}", headers=headers, timeout=10)
            data = resp.json() if resp.status_code == 200 else {}

        doc = {
            "id": repo.replace("/", "_"),
            "type": "repo",
            "title": data.get("name", repo),
            "repo": repo,
            "url": data.get("html_url", f"{GITEA_URL}/{repo}"),
            "description": data.get("description", ""),
        }
        async with httpx.AsyncClient() as client:
            await client.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=[doc],
                               headers=await meili_headers(), timeout=5)

        jlog(level="info", message="reindexado", repo=repo, event_id=event_id, request_id=request_id)
        await msg.ack()
    except Exception as e:
        jlog(level="error", message=f"fallo al procesar evento, no ack: {e}")
        # no hacemos ack -> queda para reintento / DLQ segun configuracion del stream


@app.on_event("startup")
async def startup():
    global nc
    await ensure_index()
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    for attempt in range(10):
        try:
            await js.subscribe(
                "gitea.push", durable="search-svc", cb=handle_push, manual_ack=True,
                config=ConsumerConfig(max_deliver=3, ack_wait=10),
            )
            ready["nats"] = True
            jlog(level="info", message="suscripto a gitea.push")
            break
        except Exception as e:
            jlog(level="warning", message=f"stream EVENTS aun no listo, reintento {attempt + 1}/10: {e}")
            await asyncio.sleep(2)
    else:
        jlog(level="error", message="no se pudo suscribir a gitea.push tras 10 intentos")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    if not ready["meili"] or not ready["nats"]:
        raise HTTPException(status_code=503, detail="dependencias no disponibles")
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    return f"search_indexed_total {len(seen_events)}\n"


@app.get("/v2/search")
async def search(q: str):
    if not q:
        raise HTTPException(status_code=400, detail="falta el parametro q")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"{MEILI_URL}/indexes/{INDEX}/search", json={"q": q},
                                      headers=await meili_headers(), timeout=5)
        except Exception:
            raise HTTPException(status_code=503, detail="indice de busqueda no disponible")
    if resp.status_code != 200:
        raise HTTPException(status_code=503, detail="indice de busqueda no disponible")
    hits = resp.json().get("hits", [])
    return {"query": q, "total": len(hits), "results": hits}
