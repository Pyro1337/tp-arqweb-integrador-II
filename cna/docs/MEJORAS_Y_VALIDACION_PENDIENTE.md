# Mejoras de fuente y aceptación pendiente

Este documento distingue **implementación**, **tests locales** y **experimentos del
laboratorio del equipo**. Ninguna mejora de este cambio convierte por sí sola C1–C5
en aprobados ni constituye aceptación docente de la adaptación Gitea/BookStack.
Se conserva la evidencia histórica del equipo sin reescribirla. Las fuentes nuevas
requieren imágenes reconstruidas y repetir las pruebas afectadas.

## Implementación de esta integración

- S1: eventos completados solo después de Meilisearch `succeeded` y ACK; fallos
  quedan reintentables. Presupuesto de tiempo, reintentos selectivos, concurrencia
  acotada y circuit breaker con una prueba half-open. Dedupe local, no exactly-once.
- Bridge: publicación JetStream con PubAck, captura durable de advisories y DLQ con
  bytes originales. `/dlq` requiere `X-DLQ-Token` configurado mediante
  `DLQ_READ_TOKEN`; vacío deshabilita inspección, no el procesamiento de fondo.
  Configuraciones de stream incompatibles se rechazan sin migrarlas/borrarlas.
- Exportación: Redis Streams con pendientes recuperables, renovación de lease,
  fencing por dueño/intento y objetos separados por intento. Ver
  `../services/export-svc/RECOVERY-TODO.md` para límites y migración.
- Observabilidad: logs correlacionados de S2 con request/trace ID y sin texto de
  excepciones; log JSON del gateway sin consultas/credenciales; colector de
  metadatos de colas que separa esperando entrega, en vuelo sin ACK y DLQ almacenada.
  Un error de lectura se muestra con `queue_collector_up=0`, nunca como cola vacía.
- C2: herramienta opt-in que mide finalización real y verifica el número de workers
  observados antes/después. No ejecuta escalado ni carga por defecto.
- C4: lectura por operación de token desde archivo opcional para rotación sin
  reiniciar search/worker. Es una capacidad de código; **no una rotación ejecutada**.

## Protocolo para rotar sin reiniciar (opt-in)

1. En un laboratorio autorizado, preparar `cna/deploy/secrets/gitea/` con permisos
   restrictivos. El archivo `token` debe pertenecer al UID 1001 y ser legible por
   ese UID (por ejemplo 0400). Los directorios deben permitirle atravesar la ruta.
   Ni archivo ni directorio se versionan. No pegar tokens en comandos/historial.
2. El token inicial debe tener scopes mínimos sobre el repositorio de prueba.
   Usar `docker compose -f docker-compose.yml -f compose.token-file.yml ...` al
   desplegar. **Este alta inicial puede recrear contenedores y no es sin downtime.**
3. Una vez en este modo, crear en Gitea un segundo token válido con los mismos
   permisos. Mantener válido el anterior mientras se verifica el nuevo.
4. Escribir el nuevo token en un archivo temporal dentro del MISMO directorio,
   conservar propietario/modo, sincronizarlo y renombrarlo atómicamente a `token`.
   Se monta el directorio, no el archivo individual, para observar el nuevo inode.
5. Verificar operaciones reales de búsqueda/indexación y exportación, no solo
   `/healthz`. Las operaciones nuevas releen el archivo; una operación ya iniciada
   puede seguir usando el anterior. Esperar el máximo presupuesto/lease de esas
   operaciones y comprobar que no quedan en curso antes de revocarlo.
6. Revocar el token anterior en Gitea. Probar explícitamente su rechazo y continuidad
   con el nuevo, midiendo errores y disponibilidad. No registrar sus valores.

No hay garantía automática de cero caída: depende también de Gitea, IO, permisos,
red y de completar correctamente el protocolo. Archivo ausente/vacío/inválido con
`GITEA_TOKEN_FILE` configurado falla cerrado; no vuelve al token viejo del entorno.

## Qué debe volver a medirse en las VM del equipo

- P45/P51/P53: DLQ, recuperación y redelivery con las imágenes nuevas. Los streams
  EVENTS antiguos pueden requerir una migración planificada antes del arranque.
- P55: comparar el MISMO lote/repositorio con una y tres réplicas, VM inicialmente
  en reposo, recursos/errores capturados. No convertir ejecuciones unitarias en jobs/min.
- P57–P60: captura de dashboard RED + colas, logs con el mismo request ID donde
  corresponda al MISMO flujo, traza en Jaeger y alerta realmente disparada.
  Nginx reenvía traceparent: esto no implementa un span nativo del gateway.
  Un webhook de Gitea no hereda automáticamente el request ID de una petición
  de edición que Gitea no propague; documentar esa frontera en vez de inventarlo.
- P61/P62: reconstruir/reescanear imágenes finales y medir rotación con revocación.
- Capturar UTC, `usuario@hostname`, comando, versión/digest y tiempos reales; adjuntar
  stdout/stderr/código y conclusiones propias del equipo. No editar pruebas viejas
  para hacerlas parecer ejecutadas sobre esta versión.

## Entrega

Aún corresponden al equipo el informe PDF consolidado, evidencias nuevas, demo y
respuestas individuales. No se crea `v2.0.0` hasta seleccionar y validar el commit
final. Los resultados históricos no se suman a las ejecuciones de regresión nuevas.
