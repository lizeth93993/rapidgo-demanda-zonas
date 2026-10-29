# Spark Structured Streaming (Issues #8 y #9)

`streaming_job.py` lee `rapidgo.demanda.pedidos.v1`, valida y transforma los eventos y escribe Parquet con checkpoint:

| Query | Salida | Checkpoint |
|---|---|---|
| `raw_pedidos` | `datalake/raw/pedidos` (todo, incluso inválidos) | `_checkpoints/raw_pedidos` |
| `processed_pedidos` | `datalake/processed/pedidos` (válidos, deduplicados, con franja) | `_checkpoints/processed_pedidos` |

Transformaciones: parseo con el schema del contrato, validación (`is_valid`, `motivo_invalido`), `event_ts` a timestamp y hora de Bogotá, deduplicación por `event_id` y franja horaria. Las reglas compartidas están en `reglas.py`.

Ventanas, KPIs y alertas: Issue #9.

```bash
docker compose logs -f spark      # [progreso] de cada query
```
