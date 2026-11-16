# Medición reproducible de exportaciones (C2)

`python3 benchmark_exports.py` imprime un plan y **no consulta Docker ni la red**.
El modo de ejecución genera trabajos reales: requiere autorización del laboratorio,
un socket Docker privado y contexto/proyecto llamados `arqweb-lab`, y URL loopback.
La atestación humana no demuestra ausencia de túneles/redirecciones hacia producción.
No usar este modo en el daemon global del VPS ni exponer los puertos del laboratorio.

En un laboratorio ya provisionado y autorizado (no se escala automáticamente):

```bash
# El operador deja una réplica y espera sistema ocioso.
python3 benchmark_exports.py --execute --isolated-lab --expected-workers 1 \
  --base-url http://127.0.0.1:8004 --repo equipo/prueba --count 50 > run-1.json
# El operador escala a tres, verifica salud y vuelve a esperar sistema ocioso.
python3 benchmark_exports.py --execute --isolated-lab --expected-workers 3 \
  --base-url http://127.0.0.1:8004 --repo equipo/prueba --count 50 > run-3.json
```

Se valida el número observado y la identidad de contenedores antes/después. La
medición usa estado terminal real y reloj monotónico; timeout/fallos son salida no
cero, no éxito. No se imprimen URLs firmadas ni resultados de contenido. Comparar
`jobs_per_minute`, `elapsed_seconds`, errores, recursos y tamaño/igualdad de los
artefactos en las dos corridas. Un lote terminado no prueba igualdad del contenido.
La herramienta no valida aislamiento de infraestructura ni firma/autenticidad de
la API; esas verificaciones y el análisis de resultados corresponden al equipo.

Tests offline: `python -m pytest -q cna/operations/` con pytest instalado. Sus fixtures
solo verifican el instrumento: **no constituyen un benchmark del sistema**.
