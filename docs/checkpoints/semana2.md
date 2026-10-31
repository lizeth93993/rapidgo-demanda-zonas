# Checkpoint Semana 2 - v0.2.0-streaming

**Equipo 2 · Variante 5: Demanda por zonas** · Lizeth Jimenez, Santiago Peña, Carlos Guzman

## 1. Qué quedó funcionando
Pipeline Kafka → Spark Structured Streaming → Data Lake corriendo con `docker compose up -d --build`:
- Producer con key `zone_id` y reloj simulado (x10), con escenarios de pico y de pocos repartidores.
- Spark con 5 queries y checkpoint: raw, processed (validado, deduplicado y con franja), pedidos por minuto (ventana de 1 min), zonas calientes con DEMANDA_ALTA (5 min / 1 min) y CAPACIDAD_INSUFICIENTE (15 min / 5 min).
- Parquet en `datalake/raw`, `processed` y `curated`.
- CI en cada PR: compileall, pytest (69 pruebas), dry-run del producer y `docker compose config`.

## 2. Valor real observado
-