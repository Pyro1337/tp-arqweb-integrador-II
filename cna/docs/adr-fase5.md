# ADR — Registro de Decisiones (Fase 5)

## ADR-006: Estrategia de migración e integración con el monolito

**Estatus:** Aceptado

**Contexto:** Gitea tiene que seguir andando normal mientras le vamos sacando funcionalidades para pasarlas a microservicios. No podemos ponernos a tocar su código fuente porque ahí perdemos la posibilidad de actualizar Gitea con las versiones oficiales y dejaría de ser reproducible desde cero.

**Decisión:** Vamos con el patrón Strangler Fig. La integración con Gitea es solo por su API REST (con un token de servicio) para leer datos, y por Webhooks para enterarnos quién cambia qué. No se toca ni un archivo del código de Gitea.

**Alternativa que descartamos:** Pensamos en conectarnos directo a `gitea.db` (SQLite) desde los microservicios, leyendo o haciendo polling sobre las tablas para detectar cambios. La descartamos porque SQLite no está pensado para que varios procesos externos le peguen al mismo tiempo, hay riesgo de que se bloquee o se corrompa algo, y además rompe todo el encapsulamiento: si mañana Gitea cambia una columna en una actualización, se nos cae todo.

**Consecuencias:**
- A favor: cero riesgo de romper el monolito, podemos actualizar Gitea sin que eso afecte a los microservicios.
- En contra: dependemos de que la API de Gitea esté arriba y de que no le cambien el contrato de un día para el otro; y siempre hay algo más de latencia por ir por HTTP en vez de ir directo a la base.

---

## ADR-007: Selección del broker de mensajes

**Estatus:** Aceptado

**Contexto:** Necesitamos que `event-bridge` les entregue los eventos de forma confiable a los que escuchan (S1, S2 y después quizás S4), sin perder nada si algún consumidor se cae en el medio.

**Decisión:** Nos quedamos con NATS JetStream.

**Alternativas que descartamos:**
- RabbitMQ: tiene más funciones (exchanges, ruteo más complejo) pero come bastante más RAM solo estando prendido, y con los 2-4 GB que tenemos de VM no nos sobra nada de margen.
- Redis Streams: también es liviano, pero lo de manejar ack/nack y consumer groups con durabilidad hay que armarlo más a mano, mientras que JetStream ya trae streams persistentes y reintentos de fábrica.

**Consecuencias:**
- A favor: gasta poca memoria, guarda los mensajes en disco, y la DLQ se configura sin mucho lío.
- En contra: lo conocemos menos que RabbitMQ, así que al principio el equipo tiene que invertir tiempo en aprenderlo.

---

## ADR-008: Plataforma de orquestación

**Estatus:** Aceptado

**Contexto:** Las VM que tenemos son de 2 a 4 GB de RAM, corriendo en VMware Workstation o VirtualBox según la PC de cada uno.

**Decisión:** Docker Compose en un solo nodo para VM-2, juntando ahí microservicios, broker y observabilidad.

**Alternativa que descartamos:** Docker Swarm con 2-3 nodos. Lo tuvimos en cuenta, pero con la RAM real que medimos no nos alcanza para separar bien los nodos sin que cada uno ande muy justo. Queda anotado como posible mejora si conseguimos más recursos antes de entregar el Bloque II.

**Consecuencias:**
- A favor: es más simple de levantar y de debuggear, con menos partes moviéndose, y cumple igual con el mínimo que pide el TP (al menos 1 VM con red interna de contenedores).
- En contra: nos quedamos sin los puntos extra de B5 por no usar Swarm/k3s multi-nodo, y en los experimentos de caos (X4, apagar un nodo entero) no se nota tan claro el efecto.

---

## ADR-009: Base de datos por servicio

**Estatus:** Aceptado

**Contexto:** Cada microservicio necesita guardar lo suyo (el índice de búsqueda, los registros de auditoría, los jobs de exportación) sin depender del esquema interno de Gitea.

**Decisión:** Cada servicio tiene su propio almacén de datos (database per service). Queda terminantemente prohibido que cualquier microservicio abra una conexión directa a `gitea.db`.

**Alternativa que descartamos:** Meter tablas propias adentro de la misma base SQLite de Gitea (por ejemplo una tabla `audit_log` en `gitea.db`). La descartamos por el mismo motivo del ADR-006: rompe el encapsulamiento y cualquier migración de Gitea nos puede pisar esas tablas sin avisar.

**Consecuencias:**
- A favor: cada servicio es independiente, se puede escalar o redesplegar sin tener que coordinar nada con "el equipo de Gitea" (que en este caso somos nosotros mismos, pero bueno, el principio aplica igual).
- En contra: hay datos duplicados entre Gitea y los microservicios (el título de un issue vive tanto en Gitea como en el índice de S1), así que hay que mantenerlos sincronizados vía eventos.

---

## ADR-010: Consistencia eventual e idempotencia

**Estatus:** Aceptado

**Contexto:** Los datos de S1 (índice) y S2 (auditoría) se actualizan de forma asíncrona a partir de los webhooks de Gitea, entonces nunca van a estar perfectamente al instante sincronizados con el monolito.

**Decisión:** Aceptamos un modelo de consistencia eventual (con un margen de hasta 5 segundos, según pide el CA-3). Cada evento lleva un `event_id` único (UUID) que genera `event-bridge`, y los consumidores lo usan para darse cuenta si un evento llegó repetido y descartarlo (idempotencia). Los mensajes que fallan después de N reintentos se mandan a una cola de mensajes fallidos (DLQ).

**Alternativa que descartamos:** Que Gitea llame de forma síncrona a los microservicios justo en el momento del cambio, por ejemplo bloqueando la respuesta hasta que S1 confirme que ya reindexó. La descartamos porque eso ataría la disponibilidad del monolito a la de los microservicios, justo lo contrario de lo que pide el CA-5 (degradación elegante).

**Consecuencias:**
- A favor: el monolito nunca se traba ni se cae por culpa de que un microservicio esté caído.
- En contra: hay una ventana corta donde el usuario puede buscar algo recién pusheado y todavía no aparece, porque el índice tarda un toque en actualizarse. Hay que explicar bien esto en la demo para que no parezca que es un bug.
