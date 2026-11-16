# Repetir QA de fuentes integradas

Desde el repositorio limpio, con Python 3.12 y `uv`:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r cna/requirements-qa.txt
uv pip check --python .venv/bin/python
```

Ejecutar **cada servicio en un proceso separado**: todos usan módulos llamados
`app`/`worker`, por lo que recolectarlos juntos en un solo pytest contamina imports.
Desde cada directorio `cna/services/<servicio>/`, ejecutar el intérprete del venv
por su ruta absoluta: `python -m pytest -p no:cacheprovider -q`.
Repetir en `cna/deploy`, `cna/observability` y `cna/operations`.

Mantener tracing habilitado para verificar los spans en memoria. Los conftest de
bridge/search sustituyen únicamente el exportador de red. En auditoría, dejar
`JAEGER_OTLP_ENDPOINT` vacío para no enviar trazas a la red. No deshabilitar todo
el SDK para hacer pasar pruebas de contexto.

Las integraciones con broker requieren rutas explícitas a **ejecutables privados**,
no URLs de servidores ni sockets de producción. Desde la raíz del repositorio:

```bash
# Sustituir estas rutas por binarios compatibles en el entorno local autorizado.
export BROKER_NATS_SERVER=/ruta/privada/nats-server
export ARQWEB_REDIS_SERVER=/ruta/privada/redis-server
export PYTHONDONTWRITEBYTECODE=1
unset PYTHONPATH OTEL_SDK_DISABLED OTEL_EXPORTER_OTLP_ENDPOINT
unset OTEL_EXPORTER_OTLP_TRACES_ENDPOINT OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
export JAEGER_OTLP_ENDPOINT=
PYTHON_QA="$PWD/.venv/bin/python"
(cd cna/services/search-svc && "$PYTHON_QA" -m pytest -p no:cacheprovider -q)
(cd cna/services/event-bridge && "$PYTHON_QA" -m pytest -p no:cacheprovider -q)
(cd cna/services/export-svc && "$PYTHON_QA" -m pytest -p no:cacheprovider -q)
```

El perfil de broker ejercido es NATS 2.10.24 y Redis 7.0.15. Sin la opción explícita
los casos reales se omiten: **una omisión no prueba el protocolo real**. Los fixtures
son procesos propios con recursos y tiempo acotados; no se conectan al broker del
despliegue ni deben reemplazarse por servicios compartidos o el daemon de producción.
Ejecutar también los restantes directorios indicados arriba antes de afirmar un QA
completo. Las pruebas exclusivas de revisores se registran aparte, no se suman a las
identidades únicas de la suite del repositorio.

Verificación de configuración sin acceder al daemon Docker:

```bash
cd cna/deploy
# Usar únicamente variables sintéticas o un archivo de ejemplo sin secretos.
DOCKER_HOST=unix:///tmp/arqweb-no-daemon.sock docker compose --env-file .env.example config --quiet
DOCKER_HOST=unix:///tmp/arqweb-no-daemon.sock docker compose --env-file .env.example \
  -f docker-compose.yml -f compose.token-file.yml config --quiet
```

Esto NO construye imágenes ni valida Nginx/Prometheus en ejecución. Antes de aplicar
el gateway en VM-1 se necesita `nginx -t` con su entorno TLS real. Rehacer los tests
al modificar fuentes, empaquetado, dependencias o configuración de consumidores.
Los resultados de versiones anteriores se conservan separados, sin sumar reruns.
