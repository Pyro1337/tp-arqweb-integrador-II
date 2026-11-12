# CNA — TP Integrador II (Fases 5, 6 y 7)

Continuación del TP Integrador I sobre Gitea. Evoluciona el monolito hacia una
arquitectura cloud native mediante el patrón Strangler Fig.

## Estructura

- `services/<servicio>/` — código, Dockerfile y pruebas de cada microservicio (S1-S4).
- `contracts/` — contratos OpenAPI 3.1 y AsyncAPI 3.0.
- `deploy/` — Docker Compose / stack / manifiestos de despliegue.
- `observability/` — tableros de Grafana, reglas de alertas, configuración de Prometheus/Loki/Jaeger.
- `docs/` — diagramas C4, secuencias, ADR-006 a ADR-010.

## Despliegue

_(Completar en Bloque II con la instrucción de un solo comando, ej. `docker compose up -d`.)_
