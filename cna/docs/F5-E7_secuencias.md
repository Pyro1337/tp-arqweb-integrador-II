# F5-E7 — Diagramas de secuencia

Acá van las 3 interacciones obligatorias, cada una con su camino normal y con qué pasa cuando algo falla. Los armamos pensando en lo que de verdad vamos a tener que mostrar en la demo.

## 1. Push -> indexacion (asincrona)

### Camino feliz

```mermaid
sequenceDiagram
    actor U as Usuario
    participant G as Gitea
    participant EB as event-bridge
    participant B as Broker (NATS)
    participant S1 as search-svc
    participant S2 as audit-svc

    U->>G: git push
    G->>EB: Webhook push (token compartido)
    EB->>EB: genera event_id, arma sobre de evento
    EB->>B: publica gitea.push (v1)
    B->>S1: entrega evento (ack manual)
    S1->>G: GET /api/v1/repos/{owner}/{repo} (token servicio)
    S1->>S1: reindexa en Meilisearch
    S1->>B: ack
    B->>S2: entrega evento (ack manual)
    S2->>S2: guarda registro de auditoria
    S2->>B: ack
```

### Camino de error

```mermaid
sequenceDiagram
    actor U as Usuario
    participant G as Gitea
    participant EB as event-bridge
    participant B as Broker (NATS)
    participant S1 as search-svc

    U->>G: git push
    G->>EB: Webhook push
    EB->>B: publica gitea.push
    B->>S1: entrega evento
    S1->>G: GET /api/v1/repos/... (token servicio)
    G-->>S1: timeout / 503
    S1->>S1: reintenta con backoff exponencial
    S1-->>B: no hace ack (nack)
    Note over B,S1: tras N reintentos fallidos el mensaje va a la DLQ
    B->>B: mueve mensaje a DLQ
```

## 2. Busqueda (sincrona)

### Camino feliz

```mermaid
sequenceDiagram
    actor C as Cliente
    participant N as Nginx (gateway)
    participant S1 as search-svc
    participant M as Meilisearch

    C->>N: GET /v2/search?q=nginx
    N->>N: genera X-Request-ID, rate limit ok
    N->>S1: proxy_pass con X-Request-ID
    S1->>M: consulta full-text
    M-->>S1: resultados
    S1-->>N: 200 OK (JSON)
    N-->>C: 200 OK (JSON)
```

### Camino de error

```mermaid
sequenceDiagram
    actor C as Cliente
    participant N as Nginx (gateway)
    participant S1 as search-svc

    C->>N: GET /v2/search?q=nginx
    N->>S1: proxy_pass
    S1-->>N: connection refused (contenedor caido)
    N-->>C: 503 JSON uniforme (no HTML de nginx)
```

## 3. Exportacion (asincrona por trabajos)

### Camino feliz

```mermaid
sequenceDiagram
    actor C as Cliente
    participant N as Nginx (gateway)
    participant S3 as export-svc (API)
    participant Q as Redis (cola)
    participant W as export-worker
    participant G as Gitea
    participant Mi as MinIO

    C->>N: POST /v2/exports {repo, format}
    N->>S3: proxy_pass
    S3->>Q: encola job
    S3-->>C: 202 Accepted {job_id}
    W->>Q: toma job
    W->>G: GET /api/v1/repos/... (token servicio)
    G-->>W: datos del repo/actividad
    W->>Mi: guarda archivo
    W->>Q: marca job como done
    C->>N: GET /v2/exports/{job_id}
    N->>S3: proxy_pass
    S3-->>C: 200 OK {status: done, download_url}
```

### Camino de error

```mermaid
sequenceDiagram
    actor C as Cliente
    participant S3 as export-svc (API)
    participant Q as Redis (cola)
    participant W1 as export-worker-1
    participant W2 as export-worker-2

    C->>S3: POST /v2/exports
    S3->>Q: encola job
    S3-->>C: 202 Accepted {job_id}
    W1->>Q: toma job, status=running
    Note over W1: el worker muere a mitad del proceso (docker kill)
    Q->>Q: detecta que el job quedo sin heartbeat, lo vuelve a encolar
    W2->>Q: toma el mismo job
    W2->>W2: completa el job, status=done
```
