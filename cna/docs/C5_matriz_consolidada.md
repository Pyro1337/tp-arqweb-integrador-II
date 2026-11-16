# C5 - Matriz consolidada de pruebas (P26 en adelante)

> **Alcance histórico:** las filas siguientes conservan el resultado declarado por
> el equipo antes de la integración de resiliencia/DLQ/Streams. No certifican las
> fuentes ni imágenes nuevas. P45, P51, P53, P55 y P57–P62 deben revalidarse según
> [Mejoras y aceptación pendiente](MEJORAS_Y_VALIDACION_PENDIENTE.md). Que una fila
> tenga archivo no implica aceptación docente ni elimina limitaciones registradas.

Esta matriz continúa la Tabla 5 del TP I, con las pruebas de las Fases 5, 6 y 7
del TP II (P26 a P62), tal como pide la consigna en la sección 8. Cada fila
apunta al archivo de evidencia real dentro de este repo, con el resultado
obtenido (no solo "se hizo", sino qué pasó en concreto).

| Prueba | Evidencia | Fase | Ítem | Descripción | Resultado |
|---|---|---|---|---|---|
| P26 | [F5-E1](evidencias/fase5/F5-E1_tablas_gitea.txt) | Fase 5 | A1 | Tablas por módulo vía `information_schema` | OK. Se listaron las tablas de Gitea agrupadas por módulo funcional. |
| P27 | [F5-E2](F5-E2_matriz_acoplamiento.md) | Fase 5 | A1 | Matriz de acoplamiento módulo × tabla | OK. Matriz armada, usada después para justificar los límites de los 4 bounded contexts. |
| P28 | [F5-E3](F5-E3_mapa_contextos.md) | Fase 5 | A1 | Mapa de contextos (DDD) | OK. 4 contextos delimitados (búsqueda, auditoría, export, media) con sus anticorruption layers hacia Gitea. |
| P29 | [F5-E4](evidencias/fase5/F5-E4_spectral_lint.txt) | Fase 5 | A3 | Contratos OpenAPI/AsyncAPI validados sin errores | OK. Spectral corrió sin errores sobre los 4 contratos OpenAPI y los AsyncAPI de los eventos. |
| P30 | [F5-E5/E6](F5-E5_F5-E6_C4.md) | Fase 5 | A4 | Diagrama C4 nivel 1 (contexto) | OK. |
| P31 | [F5-E5/E6](F5-E5_F5-E6_C4.md) | Fase 5 | A4 | Diagrama C4 nivel 2 (contenedores) | OK. |
| P32 | [F5-E7](F5-E7_secuencias.md) | Fase 5 | A4 | Diagramas de secuencia con caminos de error | OK. Incluye camino feliz y caminos de error (timeout, DLQ) para los 4 servicios. |
| P33 | [F6-E1](evidencias/fase6/F6-E1_docker_info.txt) | Fase 6 | B1 | Nodos activos del clúster o motor Docker | OK. VM-2 corriendo Docker Compose, 1 vCPU / ~1.6GB RAM (dato clave para los hallazgos de C2 más adelante). |
| P34 | [F6-E2](evidencias/fase6/F6-E2_ufw_vm2.txt) / [F6-E2b](evidencias/fase6/F6-E2b_ufw_vm1_regla_gitea_api.txt) | Fase 6 | B1 | `ufw status verbose` de cada nodo | OK. Reglas de ambas VMs documentadas, incluyendo la regla agregada para permitir el webhook de Gitea solo desde la IP de VM-2. |
| P35 | [F6-E3](evidencias/fase6/F6-E3_certificado_san.txt) | Fase 6 | B1 | Certificado X.509 con doble SAN | OK. |
| P36 | [F6-E4](evidencias/fase6/F6-E4_busqueda_antes.txt) | Fase 6 | B2.1 | Búsqueda vía gateway antes/después de editar | OK. Se ve la diferencia entre el resultado antes y después de indexar el cambio. |
| P37 | [F6-E5](evidencias/fase6/F6-E5_idempotencia.txt) | Fase 6 | B2.2 | Idempotencia ante evento duplicado | OK. Reenvío del mismo `X-Gitea-Delivery` no duplica el registro (constraint `ON CONFLICT DO NOTHING`). |
| P38 | [F6-E6](evidencias/fase6/F6-E6_export_estado.txt) | Fase 6 | B2.3 | Ciclo completo de un job de exportación | OK. |
| P39 | [F6-E7](evidencias/fase6/F6-E7_objeto_minio.txt) | Fase 6 | B2.3 | Objeto exportado en MinIO | OK. |
| P40 | [F6-E8](evidencias/fase6/F6-E8_media_subida.txt) | Fase 6 | B2.4 | Subida y miniatura vía gateway | OK. Incluye la miniatura descargada como PNG. |
| P41 | [F6-E9](evidencias/fase6/F6-E9_imagenes.txt) | Fase 6 | B3 | Etiquetas y tamaños de imágenes | OK. |
| P42 | [F6-E10](evidencias/fase6/F6-E10_inspect_search_svc.txt) | Fase 6 | B3 | `docker inspect`: usuario, límites, healthcheck | OK. Usuario no-root (uid 1001), límites de memoria y healthcheck confirmados. |
| P43 | [F6-E11](evidencias/fase6/F6-E11_webhook_config.txt) | Fase 6 | B4 | Webhook configurado en Gitea | OK. |
| P44 | [F6-E12](evidencias/fase6/F6-E12_evento_broker.txt) | Fase 6 | B4 | Evento recibido en el broker | OK. |
| P45 | [F6-E13](evidencias/fase6/F6-E13_dlq.txt) | Fase 6 | B4 | Mensaje enviado a la DLQ | OK. |
| P46 | [F6-E14](evidencias/fase6/F6-E14_nginx_test.txt) | Fase 6 | B5 | `nginx -t` y server block del gateway | OK. |
| P47 | [F6-E15](evidencias/fase6/F6-E15_rate_limit.txt) | Fase 6 | B5 | Respuesta 429 por rate limiting | OK. Se corrigió primero un bug donde Nginx devolvía 503 en vez de 429 (`limit_req_status`). |
| P48 | [F6-E16](evidencias/fase6/F6-E16_deploy_healthy.txt) | Fase 6 | B5 | Despliegue en un comando, servicios `healthy` | OK. |
| P49 | [F7-X1](evidencias/fase7/F7-X1_resiliencia_search.txt) | Fase 7 | C1 | X1: caída de S1 y MTTR | Hallazgo real: `docker kill` no dispara el restart nativo de Docker (confirmado con `docker events`). Corregido con un watchdog externo por cron (`cna/deploy/watchdog.sh`). |
| P50 | [F7-X2](evidencias/fase7/F7-X2_resiliencia_audit.txt) | Fase 7 | C1 | X2: cero pérdida de eventos en S2 | OK. audit-svc no pierde eventos gracias a los consumers durables de NATS JetStream con reintentos configurados. |
| P51 | [F7-X3](evidencias/fase7/F7-X3_resiliencia_export_worker.txt) | Fase 7 | C1 | X3: reintento de job tras caída de worker | OK. Se reescribió el worker para usar BRPOPLPUSH + heartbeat + reencolado de huérfanos; antes no tenía ningún mecanismo de recuperación. |
| P52 | [F7-X4](evidencias/fase7/F7-X4_resiliencia_vm2_completa.txt) | Fase 7 | C1 | X4: apagado de VM-2 y convergencia | Hallazgo real: 4 contenedores de infraestructura (meilisearch, minio, audit-db, export-redis) no tenían `restart: unless-stopped` y quedaron caídos tras el reinicio. Corregido y re-validado con un segundo apagado completo. |
| P53 | [F7-X5](evidencias/fase7/F7-X5_resiliencia_latencia.txt) | Fase 7 | C1 | X5: timeout, backoff e interruptor de circuito | OK. Se agregó circuit breaker (umbral 3, cooldown 15s) y backoff exponencial en search-svc contra la API de Gitea. |
| P54 | [F7-C2](evidencias/fase7/F7-C2_resumen.txt) | Fase 7 | C2 | Resultados k6: monolito vs. S1 | Hallazgo real: search-svc es bastante más lento que el monolito bajo carga (httpx abre una conexión nueva por request en vez de reusar un cliente, más la contención del único vCPU de VM-2 contra 12 contenedores más). Documentado como limitación conocida, no corregido por quedar fuera del alcance del TP. |
| P55 | [F7-C2](evidencias/fase7/F7-C2_resumen.txt) | Fase 7 | C2 | Escalado de `export-worker` 1 → 3 | OK con matiz: la primera corrida con la VM saturada por el benchmark anterior tardó 3m40s; repetida con el sistema en reposo, 50 exportaciones se completaron en menos de 15s con 3 réplicas. Confirma que el diseño de colas escala bien, el límite real es CPU de la VM. |
| P56 | [F7-C2](evidencias/fase7/F7-C2_docker_stats.txt) | Fase 7 | C2 | Consumo de recursos por nodo | Hallazgo real: MinIO usando 98% de su límite de memoria (251.7MiB de 256MiB). Corregido subiendo el límite a 384MB. |
| P57 | [red-dashboard.json](../observability/grafana/provisioning/dashboards/red-dashboard.json) | Fase 7 | C3 | Tablero RED en Grafana | OK, validado en vivo (métricas de Rate/Errors/Duration por servicio vía Prometheus). No se guardó captura de pantalla como evidencia adicional, pero el dashboard queda provisionado automáticamente al levantar el stack. |
| P58 | [F7-C3](evidencias/fase7/F7-C3_logs_correlacionados.txt) | Fase 7 | C3 | Logs correlacionados por `request_id` | OK. |
| P59 | [F7-C3](evidencias/fase7/F7-C3_traza_distribuida.txt) | Fase 7 | C3 | Traza distribuida en Jaeger | OK. Propagación de contexto W3C inyectada en el envelope de NATS, visible de punta a punta desde event-bridge hasta search-svc/audit-svc. |
| P60 | [F7-C3](evidencias/fase7/F7-C3_alerta_disparada.txt) | Fase 7 | C3 | Alerta disparada | OK. Se disparó la alerta de Prometheus (tasa de errores 5xx alta / DLQ con mensajes). |
| P61 | [trivy/](evidencias/fase7/trivy/) y [sbom/](evidencias/fase7/sbom/) | Fase 7 | C4 | Informe Trivy y SBOM | Hallazgo real: 4 CVEs CRITICAL en Pillow 10.4.0 dentro de media-svc. Corregido subiendo a Pillow 13.0.0, re-testeado (3 passed) y re-escaneado (0 CRITICAL). SBOM en formato CycloneDX para las 5 imágenes. |
| P62 | [F7-C4](evidencias/fase7/F7-C4_rotacion_token.txt) | Fase 7 | C4 | Rotación del token de API | Hallazgo real: `export-worker` (3 réplicas) rotó el token sin perder ningún job en curso; `search-svc` (1 réplica, puerto fijo) tuvo un corte real de ~20 segundos. Documentado como limitación aceptada de la arquitectura (ver [REGISTRO_HALLAZGOS.md](REGISTRO_HALLAZGOS.md), H5). |

## Resumen

De las 37 pruebas (P26-P62), todas tienen evidencia verificable en el repo.
7 de ellas (P49, P52, P54, P56, P61, P62, y P55 con matiz) encontraron un
problema real durante la prueba; de esos, 5 se corrigieron y 2 quedaron
documentados como limitación aceptada por decisiones de arquitectura ya
justificadas en los ADR (ADR-008 principalmente, por los recursos limitados
de la VM). El detalle de cada hallazgo está en
[REGISTRO_HALLAZGOS.md](REGISTRO_HALLAZGOS.md).
