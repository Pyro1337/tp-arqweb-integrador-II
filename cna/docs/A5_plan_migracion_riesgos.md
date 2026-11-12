# A5 — Plan de migración y matriz de riesgos

## En qué orden vamos a extraer

1. **Primero S2 (audit-svc).** Es el que menos riesgo tiene: solo escucha eventos, no le muestra nada crítico al usuario, y si se cae no afecta ninguna función que el usuario note. Además nos sirve para probar toda la cadena Webhook -> event-bridge -> broker antes de meter algo que sí se note si falla.
2. **Después S1 (search-svc).** Ya es algo que el usuario ve (la búsqueda), pero si llega a fallar el impacto es chico: se pierde la búsqueda nueva, no la nativa de Gitea, que sigue andando igual. Reusamos la misma cadena de eventos que ya probamos con S2.
3. **Tercero S3 (export-svc).** Es el más complicado de los tres (tiene API + cola + workers + guardado de archivos), así que lo dejamos para cuando ya tengamos más rodaje con el patrón de contenedores gracias a S1 y S2.
4. **Por último, si llegamos con el tiempo, S4 (media-svc).** Suma puntos para la nota máxima pero no es obligatorio, entonces queda condicionado a cómo venimos de cronograma.

¿Cuándo consideramos que un paso está "hecho"? Cuando el servicio cumple los requisitos comunes de B2 (healthz/readyz/metrics, logs en JSON, mínimo 3 tests) y además la interacción específica de ese servicio funciona de punta a punta con su evidencia correspondiente (F6-E4 a F6-E8 según el caso).

## Cómo revertimos si algo sale mal

Si algún microservicio nos arruina la demo o genera algún lío en vivo, la solución es desactivar esa ruta en el gateway. Comentamos el bloque `location /v2/...` correspondiente en el server block de `api.gitea.local` y recargamos nginx:

```bash
sudo nano /etc/nginx/sites-available/api.gitea.local
sudo nginx -t
sudo systemctl reload nginx
```

Esto se hace en menos de un minuto y el resto del sistema (Gitea y los demás microservicios) sigue funcionando sin que se note nada raro.

## Matriz de riesgos

| Riesgo | Probabilidad | Impacto | Cómo lo mitigamos |
|---|---|---|---|
| Que la consistencia eventual se note para el usuario (busca algo recién pusheado y todavía no aparece) | Alta | Bajo | Ya lo dejamos documentado en el ADR-010, y en la demo explicamos el margen de 5 segundos que permite el CA-3 |
| Quedarnos cortos de RAM en VM-2 (tenemos solo 2-4 GB) | Alta | Alto | Por eso elegimos NATS JetStream en vez de RabbitMQ (ADR-007) y Compose en un solo nodo en vez de Swarm (ADR-008); además le ponemos límite de memoria a cada contenedor |
| Latencia de red entre VM-1 y VM-2 | Media | Medio | La medimos en C2 contra la línea base del TP1; S1 también tiene timeout y circuit breaker para no quedarse colgado si la API de Gitea tarda |
| Secretos compartidos (el token de Gitea, el secreto del webhook) | Media | Alto | Van por `.env` que no se versiona, y en C4 (seguridad) documentamos cómo se rota el token |
| Que el gateway (Nginx en VM-1) sea un punto único de falla | Baja | Alto | Es el mismo riesgo que ya teníamos desde el TP1 con el monolito. No armamos un segundo gateway por ahora, lo dejamos documentado como riesgo que aceptamos dado el alcance del TP |
