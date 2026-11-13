# Checkpoint Semana 2 - v0.2.0-streaming

**Equipo 2 · Variante 5: Demanda por zonas** · Lizeth Jimenez, Santiago Peña, Carlos Guzman

## 1. Qué quedó funcionando
Pipeline Kafka → Spark Structured Streaming → Data Lake corriendo con `docker compose up -d --build`:
- Producer con key `zone_id` y reloj simulado (x10), con escenarios de pico y de pocos repartidores.
- Spark con 5 queries y checkpoint: raw, processed (validado, deduplicado y con franja), pedidos por minuto (ventana de 1 min), zonas calientes con DEMANDA_ALTA (5 min / 1 min) y CAPACIDAD_INSUFICIENTE (15 min / 5 min).
- Parquet en `datalake/raw`, `processed` y `curated`.
- CI en cada PR: compileall, pytest (69 pruebas), dry-run del producer y `docker compose config`.

## 2. Valor real observado
Corrida de 91 minutos con el producer (reloj x10) y las 5 queries de Spark (datos del [ADR-002](../decisions/ADR-002-ajuste-tras-ejecucion.md)):
- Línea `[FINAL]` del producer: `p0=38,094 (19 %), p1=97,964 (50 %), p2=59,381 (30 %), errores=0` → 195 439 mensajes confirmados por Kafka, sin errores.
- Tabla "Eventos por partición" de `resumen.py` sobre `raw/pedidos`: p0 = 18 814 (19.6 %), p1 = 47 894 (50.0 %), p2 = 29 086 (30.4 %), 95 794 eventos en total.
- Velocidad de Spark (`[progreso]`): raw 76.7, processed 31.4, kpi_demanda_minuto 74.9, zonas_calientes 42.8 y capacidad 113.1 filas/s, con los watermarks avanzando.

## 3. Decisión técnica
Se mantiene key `zone_id` con 3 particiones ([ADR-002](../decisions/ADR-002-ajuste-tras-ejecucion.md)). La partición más cargada (p1, 6 zonas) lleva el 50 % de los eventos, 1.5 veces el promedio: justo en el límite del criterio del ADR-001, y sin retraso visible en Spark, así que no se cambia a asignación explícita. Se conserva el orden de los eventos por zona.

## 4. Problema real y cómo se resolvió
Para aplicar el criterio del ADR-001 había que medir el retraso (lag) del consumidor, pero Kafka UI no lo mostraba: Spark Structured Streaming guarda sus offsets en su propio checkpoint (`datalake/_checkpoints`) y no en un consumer group de Kafka. Se resolvió midiendo el avance desde Spark (filas/s y watermark de cada query en las líneas `[progreso]`) y comparándolo con los mensajes por partición que confirma el producer. Quedó documentado en el ADR-002 (PR #28, completado en #29).

## 5. Enlaces
- Issues: https://github.com/lizeth93993/rapidgo-demanda-zonas/milestone/2
- PRs: #25 producer · #24 reglas, pruebas y CI · #26 Spark lectura · #27 Spark ventanas y alertas · #28 ADR-002 y checkpoint (#29 completa el ADR-002)
- GitHub Actions (CI en cada PR): https://github.com/lizeth93993/rapidgo-demanda-zonas/actions/workflows/ci.yml
- Release: https://github.com/lizeth93993/rapidgo-demanda-zonas/releases/tag/v0.2.0-streaming
