# CNA — TP Integrador II (Fases 5, 6 y 7)

Esta carpeta es la continuación del TP Integrador I, ahora sobre Gitea. La idea es ir evolucionando el monolito hacia una arquitectura cloud native usando el patrón Strangler Fig, sin tocar el código fuente de Gitea en ningún momento.

## Cómo está organizado

- `services/<servicio>/` — el código, Dockerfile y tests de cada microservicio (S1 a S4).
- `contracts/` — los contratos OpenAPI 3.1 y AsyncAPI 3.0.
- `deploy/` — Docker Compose / stack / manifiestos para levantar todo.
- `observability/` — tableros de Grafana, reglas de alertas y config de Prometheus/Loki/Jaeger.
- `docs/` — diagramas C4, secuencias y los ADR del 006 al 010.

## Cómo se levanta

En **el laboratorio autorizado del equipo**, después de preparar las variables y
el gateway según [`deploy/RUNBOOK.md`](deploy/RUNBOOK.md):

```bash
cd cna/deploy
cp .env.example .env
# Editar .env: IPs y credenciales reales, sin versionarlo.
docker compose --env-file .env up -d --build
```

Si ya existe una instalación anterior, no actualizar ciegamente: drenar primero
las colas legacy de exportación y revisar compatibilidad de los streams NATS.
Las configuraciones incompatibles se rechazan sin borrar mensajes. Redis usa AOF
`everysec`: puede perder aproximadamente el último segundo ante un fallo abrupto;
no se declara durabilidad ante apagón ni HA de VM por tener un volumen.

El detalle de las mejoras, migración/rotación y pruebas que deben repetirse está en
[`docs/MEJORAS_Y_VALIDACION_PENDIENTE.md`](docs/MEJORAS_Y_VALIDACION_PENDIENTE.md).
El colector de colas no publica puertos; Prometheus lo consulta por la red interna.
`DLQ_READ_TOKEN` es opcional y exclusivo de inspección: configurarlo explícitamente
para consultar `/dlq` con `X-DLQ-Token`. No reutilizar el secreto del webhook.

**Tests locales no equivalen a imágenes construidas ni aceptación de C1–C5.**
Las evidencias históricas se preservan; las imágenes y experimentos afectados
deben reconstruirse y repetirse en las VM. No se incluye ningún secreto real.
