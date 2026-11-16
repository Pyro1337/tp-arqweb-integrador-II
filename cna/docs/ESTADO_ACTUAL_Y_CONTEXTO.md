# Estado actual del proyecto — contexto para seguir con la Fase 7

> Documento histórico de cierre del Bloque II. La Fase 7 ya tiene evidencia del
> equipo en este repositorio. Para los cambios de fuente posteriores y su estado
> de aceptación consultar [Mejoras y validación pendiente](MEJORAS_Y_VALIDACION_PENDIENTE.md)
> y la matriz C5; las listas «todavía no existe» de abajo describen ese momento.

Este archivo es para que cualquiera del equipo pueda retomar el trabajo sin tener que preguntar todo de nuevo. Lo escribo al cierre del Bloque II, antes de arrancar la Fase 7.

## 1. Qué app elegimos y por qué

Seguimos con **Gitea** (la misma app del TP Integrador I), no cambiamos a otra cosa. Está desplegada en la VM-1 desde el TP1, con Nginx + TLS + UFW ya configurados de la Fase 4.

## 2. Topología actual

| Nodo | IP | Qué tiene |
|---|---|---|
| **VM-1** (`equipo01`) | `192.168.0.15` | Gitea (monolito), Nginx (gateway + edge), certificado TLS con doble SAN (`equipo01.arqweb.test` y `api.equipo01.arqweb.test`) |
| **VM-2** (`cna-node1`) | `192.168.0.20` | Los 4 microservicios + event-bridge + NATS + Meilisearch + PostgreSQL + Redis + MinIO, todo con Docker Compose |

Ojo: las IPs pueden cambiar si las VMs se reinician y usan DHCP. Si algo no conecta, lo primero es chequear `ip addr` en cada VM y actualizar lo que haga falta (sobre todo el `.env` de `cna/deploy/` y la config de Nginx).

Usuario de ambas VMs: `arqweb2026`. Las contraseñas no van en este archivo — están en el gestor de contraseñas del equipo o pedírselas a quien las configuró.

## 3. Qué está hecho (Bloque I y II, 70 de 100 puntos)

### Bloque I — Fase 5 (25 pts, completo)
Todo en `cna/docs/`: análisis de módulos de Gitea, matriz de acoplamiento, mapa de contextos DDD, los 5 ADR (006 a 010), contratos OpenAPI/AsyncAPI en `cna/contracts/`, diagramas C4 y de secuencia.

### Bloque II — Fase 6 (45 pts, completo)
- **S1 `search-svc`**: indexa repos de Gitea en Meilisearch, reindexa por push, expone `/v2/search`.
- **S2 `audit-svc`**: guarda auditoría en PostgreSQL, idempotente por `event_id`.
- **S3 `export-svc`**: API + worker separado, exporta issues a CSV, guarda en MinIO con URL prefirmada.
- **S4 `media-svc`**: sube imágenes a MinIO, genera miniatura automática.
- **`event-bridge`**: recibe los Webhooks de Gitea, los valida por secreto en la URL, los publica a NATS JetStream, y tiene un endpoint `/dlq` que registra los mensajes que agotaron los reintentos.
- Gateway en Nginx (`api.equipo01.arqweb.test`): rate limiting (429), respuesta 503 JSON uniforme si un servicio cae.
- Todo el código está en `cna/services/<servicio>/`, el despliegue en `cna/deploy/docker-compose.yml`.
- Los 16 tests automatizados (3-4 por servicio) están en cada carpeta de servicio como `test_app.py`.

**Cómo desplegar todo desde cero** (si hay que rehacer la VM-2):
```bash
cd cna/deploy
cp .env.example .env
# completar GITEA_URL, GITEA_TOKEN, WEBHOOK_SECRET, VM2_IP, y las claves de Postgres/MinIO
docker compose --env-file .env up -d --build
```
El detalle paso a paso completo está en `cna/deploy/RUNBOOK.md`.

### Cosas raras que nos encontramos (para no perder tiempo repitiendo el error)
- **MinIO sacó sus imágenes de Docker Hub y quay.io** (cambio de licencia reciente). Usamos `bitnamilegacy/minio:latest` en su lugar, que sigue siendo pública. Esta versión de MinIO **no tiene consola web** en el puerto 9001 — para ver los objetos usamos `docker exec deploy-minio-1 mc ls local/<bucket>`.
- Las URLs prefirmadas de MinIO hay que generarlas con un cliente apuntando a la IP pública de VM-2 (`192.168.0.20:9000`), no al nombre interno de Docker (`minio:9000`), porque si no no sirven para nadie fuera de la red de contenedores. Y hay que pasarle `region="us-east-1"` a mano al cliente de MinIO, si no intenta auto-detectar la región y se cuelga (problema de NAT en loop).
- El puerto 3000 de Gitea está bloqueado por UFW desde el TP1 (a propósito). Tuvimos que agregar una regla puntual para que **solo VM-2** pueda llegar a ese puerto, porque los microservicios necesitan pegarle directo a la API de Gitea.
- Gitea bloquea webhooks hacia redes privadas por defecto (protección anti-SSRF). Hay que agregar `[webhook] ALLOWED_HOST_LIST = 192.168.0.0/24` en `/etc/gitea/app.ini` de VM-1.
- El `event_id` de los eventos usa el `X-Gitea-Delivery` que manda Gitea (no un UUID random nuestro), para que la idempotencia funcione de verdad si Gitea reintenta una entrega.
- Python 3.14 (el que viene por defecto en Ubuntu 26.04) todavía no tiene paquetes precompilados para `asyncpg`, `pydantic-core` ni `Pillow`. Por eso los tests se corren dentro de un contenedor `python:3.12-slim`, no con el Python nativo de la VM.

## 4. Qué falta — Fase 7 (30 pts)

Ver el detalle completo en la conversación, resumen rápido:

- **C1 (8 pts)**: 5 experimentos de caos (matar S1, parar audit-svc 2 min, matar un worker de export a mitad de job, apagar VM-2 entera, latencia artificial hacia Gitea). Para X5 hace falta **agregar código nuevo**: timeout + backoff exponencial + circuit breaker en `search-svc`, que hoy no existe.
- **C2 (6 pts)**: benchmarks con k6/ab comparando el monolito vs `search-svc`, escalar `export-worker` a 3 réplicas, medir recursos.
- **C3 (7 pts)**: el bloque más grande — hay que desplegar Prometheus + Grafana + Loki + Jaeger (no existen todavía), instrumentar de verdad los `/metrics` de los 5 servicios (hoy son un placeholder), agregar trazas con OpenTelemetry, armar un dashboard y una alerta.
- **C4 (4 pts)**: escanear las imágenes con Trivy, generar SBOM con Syft, rotar el `GITEA_TOKEN` sin downtime, documentar 2 hallazgos nuevos.
- **C5 (5 pts)**: extender la matriz de pruebas (P26 en adelante) — esto sí lo puede hacer cualquiera sin necesitar la VM, en base a las evidencias que ya están en `cna/docs/evidencias/`.

## 5. Sobre trabajar en equipo esta parte

El sistema corre en mis VMs locales (red `192.168.0.x`), así que alguien en otra PC/red **no se puede conectar directo**. Para esta última parte conviene:

- Si alguien necesita meter mano en la VM de verdad: instalar **Tailscale** en ambas VMs y que esa persona instale el cliente en su PC — así se conecta por SSH como si estuviera en la misma red, sin duplicar nada.
- Si no, repartir así: quien tiene acceso a la VM hace C1/C2/C3/C4 (necesitan la infraestructura real), y el resto ayuda con C5 (la matriz de pruebas, que se arma con las evidencias ya generadas) y con investigar configuraciones de antemano (ej. cómo armar un dashboard RED en Grafana, qué reglas de Trivy usar).

Para la defensa final, **todo tiene que demostrarse desde un solo despliegue**, así que no conviene que cada uno arme su propia copia del sistema — mejor centralizar en una sola VM (la que ya está funcionando) y dar acceso a los demás por Tailscale si hace falta.

## 6. Dónde está todo en el repo

```
cna/
├── services/<s1-servicio>/     # código + Dockerfile + tests de cada microservicio
├── contracts/                  # OpenAPI + AsyncAPI
├── deploy/                     # docker-compose.yml, .env.example, RUNBOOK.md, config de Nginx
├── docs/
│   ├── evidencias/
│   │   ├── fase5/
│   │   ├── fase6/
│   │   └── fase7/              # vacía todavía, acá va la evidencia de C1-C5
│   └── (ADR, C4, secuencias, este mismo archivo)
```

Commits de Fase 6 repartidos: infraestructura/gateway/broker a nombre de Ivan (el usuario de este repo), los 4 microservicios a nombre de Rodrigo Ovelar.
