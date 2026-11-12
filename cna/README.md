# CNA — TP Integrador II (Fases 5, 6 y 7)

Esta carpeta es la continuación del TP Integrador I, ahora sobre Gitea. La idea es ir evolucionando el monolito hacia una arquitectura cloud native usando el patrón Strangler Fig, sin tocar el código fuente de Gitea en ningún momento.

## Cómo está organizado

- `services/<servicio>/` — el código, Dockerfile y tests de cada microservicio (S1 a S4).
- `contracts/` — los contratos OpenAPI 3.1 y AsyncAPI 3.0.
- `deploy/` — Docker Compose / stack / manifiestos para levantar todo.
- `observability/` — tableros de Grafana, reglas de alertas y config de Prometheus/Loki/Jaeger.
- `docs/` — diagramas C4, secuencias y los ADR del 006 al 010.

## Cómo se levanta

_(lo vamos a completar luego en el Bloque II, cuando tengamos el comando único armado, tipo `docker compose up -d`.)_ -> De ese punto se encarga Jose 
