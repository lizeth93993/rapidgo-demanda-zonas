# Spark Structured Streaming (Semana 2 - Issues #8 y #9)

Plan de procesamiento:
1. Leer `rapidgo.demanda.pedidos.v1` y parsear el JSON con el schema del contrato.
2. Transformaciones: validacion de contrato (`is_valid`), conversion de `event_ts` a timestamp, deduplicacion por `event_id`, calculo de `hora_local` y `franja` (America/Bogota).
3. Agregaciones por ventana con watermark de 2 min: pedidos por minuto por zona y ventanas deslizantes para las alertas.
4. Reglas de alerta con los umbrales de `config/umbrales.yaml`.
5. Escritura en Parquet en `datalake/` con checkpoint por query.
