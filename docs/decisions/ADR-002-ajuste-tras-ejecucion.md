## Opción B · Balance real de las particiones de Kafka

### Contexto / problema
El ADR-001 previó que, con key `zone_id` y 3 particiones, una partición recibiría ~50 % de la demanda (Usaquén y Suba juntas). Lo medimos con el pipeline real.

### Evidencia utilizada
Tabla "Eventos por partición de Kafka" de `resumen.py` tras 91 minutos de ejecución:

| Partición | Eventos | % | Zonas |
|---|---|---|---|
| 0 | 18,814 | 19.6 % | 4 |
| 1 | 47,894 | 50.0 % | 6 |
| 2 | 29,086 | 30.4 % | 3 |

Línea `[FINAL]` del log del producer: `p0=38,094 (19 %), p1=97,964 (50 %), p2=59,381 (30 %), errores=0`.

`filas/s` y duración de los batches de Spark en