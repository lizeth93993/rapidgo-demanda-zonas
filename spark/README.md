# Spark Structured Streaming (Issues #8 y #9)

| Archivo | Qué hace |
|---|---|
| `streaming_job.py` | Job principal: lee Kafka, transforma, agrega por ventanas, evalúa alertas y escribe Parquet con checkpoint. |
| `reglas.py` | Reglas de negocio en Python puro (franja, índice de calor, presión, severidades). Las usa el job y las prueba `tests/test_reglas.py` en CI. |
| `resumen.py` | Imprime la evidencia del Data Lake: filas por capa, eventos por partición, inválidos y alertas. |
| `Dockerfile` | Imagen `apache/spark:3.5.6` + PyYAML. El conector de Kafka se descarga al iniciar. |

## Queries

| # | Query | Entrada | Salida | Checkpoint |
|---|---|---|---|---|
| 1 | `raw_pedidos` | todo lo que llega | `datalake/raw/pedidos` | `_checkpoints/raw_pedidos` |
| 2 | `processed_pedidos` | válidos, deduplicados por `event_id` | `datalake/processed/pedidos` (por fecha y zona) | `_checkpoints/processed_pedidos` |
| 3 | `kpi_demanda_minuto` | CREATED, ventana tumbling de 1 min | `datalake/curated/kpi_demanda_minuto` | `_checkpoints/kpi_demanda_minuto` |
| 4 | `zonas_calientes` | CREATED, ventana de 5 min cada 1 min | `curated/kpi_zonas_calientes` + alertas DEMANDA_ALTA | `_checkpoints/zonas_calientes` |
| 5 | `capacidad` | todos los válidos, ventana de 15 min cada 5 min | alertas CAPACIDAD_INSUFICIENTE | `_checkpoints/capacidad` |

Las alertas se escriben en `datalake/curated/alertas`, se publican en el topic `rapidgo.demanda.alertas.v1` y aparecen en el log como `[ALERTA] ...`.

**Transformaciones:** (1) parseo del JSON con el schema del contrato; (2) validación → `is_valid` y `motivo_invalido`; (3) `event_ts` → timestamp y hora local de Bogotá; (4) deduplicación por `event_id` con watermark; (5) franja horaria.

**Watermark de 2 min:** cada ventana se escribe una sola vez, cuando el tiempo de los eventos ya pasó su fin más 2 minutos. Por eso las alertas aparecen con un pequeño retraso (con el reloj x10 del producer, unos segundos reales).

## Comandos útiles

```bash
docker compose logs -f spark                       # progreso y alertas
docker compose logs spark | grep ALERTA            # solo alertas
docker compose exec spark /opt/spark/bin/spark-submit /app/spark/resumen.py
```

Prueba local sin Kafka (requiere PySpark 3.5 y Java 17):

```bash
python producer/producer.py --dry-run --max-eventos 5000 > /tmp/eventos/lote.jsonl
python spark/streaming_job.py --fuente archivos --entrada /tmp/eventos --datalake /tmp/dl --duracion 120
```
