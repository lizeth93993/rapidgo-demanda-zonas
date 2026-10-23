# Data Lake - Variante 5: Demanda por zonas

Almacenamiento local en Parquet (montado como volumen en Spark desde la Semana 2). Los datos generados **no** se versionan; solo esta estructura.

| Capa | Ruta | Contenido | Particionado | Escribe |
|---|---|---|---|---|
| Raw | `raw/pedidos/` | Evento tal como llega de Kafka + metadatos (`kafka_partition`, `kafka_offset`, `ingest_ts`). Incluye eventos invalidos con `is_valid=false`. | `fecha` | Spark streaming |
| Processed | `processed/pedidos/` | Eventos validos, tipados, deduplicados por `event_id`, con `event_ts` como timestamp, `hora_local` y `franja`. | `fecha`, `zone_id` | Spark streaming |
| Curated | `curated/kpi_demanda_minuto/` | Pedidos por minuto por zona (ventana tumbling 1 min). | `fecha` | Spark streaming |
| Curated | `curated/kpi_zonas_calientes/` | Indice de calor por zona (ventana 5 min / 1 min). | `fecha` | Spark streaming |
| Curated | `curated/kpi_demanda_franja/` | Pedidos por fecha, franja y zona. | `fecha` | Airflow (agregacion diaria) |
| Curated | `curated/alertas/` | Alertas `DEMANDA_ALTA` y `CAPACIDAD_INSUFICIENTE` con severidad. | `fecha` | Spark streaming |
| Control | `_checkpoints/<query>/` | Checkpoints de Spark Structured Streaming (offsets y estado). | - | Spark |

Reglas:
- Raw nunca se modifica; si una regla cambia, processed y curated se pueden reconstruir desde raw.
- Toda tabla incluye `team_id` para identificar los datos del equipo.
- Airflow (Semana 3) valida raw y curated antes de generar el reporte.
