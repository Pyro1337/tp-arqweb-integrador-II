# Registro de hallazgos - Fase 7

Este documento junta todos los hallazgos reales que encontramos haciendo las pruebas
de caos, carga, observabilidad y seguridad del Bloque III. La idea es dejar
constancia de los problemas que aparecieron de verdad al probar el sistema, no solo
los casos felices, tal como lo pide la consigna.

Para cada hallazgo se pone: que esperabamos, que pasó realmente, la causa raiz, y si
se corrigió o quedó como limitación aceptada (con el porqué).

## H1 - Docker no reinicia los contenedores al matarlos con `docker kill` (X1)

**Esperado:** con `restart: unless-stopped` configurado en el compose, si un contenedor
muere el motor de Docker lo debería levantar solo.

**Obtenido:** al matar `search-svc` con `docker kill`, el contenedor queda en estado
`exited` y nunca vuelve a arrancar. Confirmado viendo `docker events`: aparece el
evento `die` pero nunca un `start` posterior, y `RestartCount` queda en 0.

**Causa raiz:** el motor de Docker de esta instalación no dispara la politica de
restart cuando el proceso principal recibe SIGKILL directo via `docker kill` (en
vez de que el proceso muera "solo" por un crash). Es un comportamiento conocido de
algunas versiones/configuraciones de Docker Engine, no un bug nuestro.

**Corregido:** si. Se armó un watchdog externo (`cna/deploy/watchdog.sh`) que corre
cada minuto por cron y reinicia cualquier contenedor del proyecto que esté en estado
`exited`. No depende de la politica nativa de Docker.

## H2 - 4 contenedores de infraestructura sin politica de restart (X4)

**Esperado:** todos los servicios (propios y de infraestructura) deberían volver a
levantar solos despues de apagar y encender la VM.

**Obtenido:** al reiniciar la VM-2 completa para la prueba de caos X4, `meilisearch`,
`minio`, `audit-db` y `export-redis` quedaron caídos. Los 5 servicios propios y NATS
si volvieron solos.

**Causa raiz:** al armar el compose original, el `restart: unless-stopped` se agregó
a NATS y a los 5 microservicios propios, pero se olvidó agregarlo a los 4 contenedores
de infraestructura de terceros. Un descuido de configuración, no un límite del
sistema.

**Corregido:** si. Se agregó `restart: unless-stopped` a los 4 servicios faltantes y
se validó con un segundo reinicio completo de la VM que todo levanta solo.

## H3 - Uso de memoria de MinIO cerca del límite (X4)

**Esperado:** los límites de memoria puestos en el compose deberían tener margen
razonable para el uso normal del servicio.

**Obtenido:** revisando `docker stats` despues de las pruebas de caos, MinIO estaba
usando cerca del 98% de su límite de memoria asignado (256MB).

**Causa raiz:** el límite se puso bajo pensando en un uso minimo, sin considerar el
overhead propio del proceso de MinIO en reposo.

**Corregido:** si. Se subió el límite de memoria de MinIO de 256MB a 384MB en el
compose.

## H4 - 4 vulnerabilidades CRITICAL en Pillow dentro de media-svc (C4)

**Esperado:** las imagenes de los servicios propios no deberían tener vulnerabilidades
de severidad CRITICAL en sus dependencias.

**Obtenido:** el escaneo con Trivy sobre la imagen de `media-svc` encontró 4 CVEs de
severidad CRITICAL, las 4 en la libreria Pillow 10.4.0 (vulnerabilidades de
desborde de buffer en el decodificador de imagenes TIFF/FPX/JPEG, explotables con
un archivo de imagen manipulado subido por el usuario).

**Causa raiz:** `media-svc` usa Pillow para generar thumbnails de las imagenes que
suben los usuarios, y la version fijada en `requirements.txt` (10.4.0) es anterior
a los parches de seguridad.

**Corregido:** si. Se subió Pillow a la version 13.0.0 (que incluye los parches), se
corrió de nuevo la suite de tests del servicio (3 passed) para confirmar que no se
rompió nada, se reconstruyó la imagen y se volvió a escanear con Trivy: quedó en 0
CRITICAL. Reportes en `cna/docs/evidencias/fase7/trivy/`.

## H5 - Rotación de token de Gitea no es zero-downtime en search-svc (C4)

**Esperado:** al rotar el `GITEA_TOKEN` (buena práctica de seguridad: credenciales
de vida corta) y recrear los contenedores con el valor nuevo, el servicio no debería
dejar de responder en ningún momento, similar a lo logrado con `export-worker`
gracias a tener varias réplicas.

**Obtenido:** se midió con un loop de `curl` a `/healthz` cada 100ms durante la
recreación del contenedor de `search-svc`. Resultado: aproximadamente 20 segundos
completos sin respuesta (durante toda la ventana medida el servicio devolvió
timeout de conexión). Comparando con `export-worker` (3 réplicas), que rotó su
token sin perder ningún job en curso (`job-b3474454` terminó bien), la diferencia
es clara.

**Causa raiz:** `search-svc` corre con una sola réplica y un mapeo de puerto fijo
en el host (`8002:8000`). Esto impide levantar una segunda instancia en paralelo
para hacer un rolling update (el intento de escalar a 2 réplicas falló con
`port is already allocated`). Además, el arranque de la app tarda varios segundos
en sí mismo (conexión a NATS, creación de índice en Meilisearch) antes de que el
puerto empiece a aceptar conexiones, lo que extiende la ventana de downtime.

**Corregido:** no, queda como limitación aceptada. Para lograr una rotación sin
downtime real haría falta correr al menos 2 réplicas detrás de un upstream de
Nginx con varios `server` (no uno fijo), o migrar a un orquestador con rolling
update nativo (Docker Swarm o k3s) — que es justamente la alternativa que el
ADR-008 descartó por los recursos limitados de la VM. Se documenta como riesgo
aceptado de la arquitectura actual, no como algo a arreglar dentro del alcance de
este TP.

## Resumen

| # | Hallazgo | Severidad | Estado |
|---|----------|-----------|--------|
| H1 | Restart policy no dispara con `docker kill` | Alta | Corregido (watchdog) |
| H2 | 4 contenedores sin `restart: unless-stopped` | Alta | Corregido |
| H3 | MinIO al 98% de memoria | Media | Corregido |
| H4 | 4 CVEs CRITICAL (Pillow) en media-svc | Critica | Corregido |
| H5 | Rotación de token sin zero-downtime en search-svc | Media | Limitación aceptada |

Los hallazgos H4 y H5 son los que se agregaron específicamente durante el bloque
de seguridad (C4) de esta fase, cumpliendo con el requisito de ampliar el registro
con al menos dos hallazgos nuevos. H1, H2 y H3 ya habían sido documentados en las
pruebas de resiliencia y escalado (X1-X5, C2) pero se incluyen aquí para tener todo
junto en un solo lugar.
