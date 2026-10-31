
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

`filas/s` y duración de los batches de Spark en el log (`docker compose logs spark | grep progreso`): entre 31 y 113 filas/s en las 5 queries (raw 76.7, processed 31.4, kpi_demanda_minuto 74.9, zonas_calientes 42.8, capacidad 113.1), con los watermarks avanzando.

### Opciones consideradas
1. Mantener key `zone_id` con 3 particiones (ADR-001).
2. Asignación explícita zona → partición (reparto casi perfecto, más código).
3. Más particiones.

### Decisión tomada
Se mantiene key `zone_id` con 3 particiones. La partición más cargada (p1) lleva 50.0 % de los eventos, 1.5 veces el promedio de 33.3 %, justo en el límite del criterio del ADR-001 (cambiar solo si supera 1.5 veces el promedio y se ve retraso en Spark). [ESCRIBE AQUÍ LA FRASE DEL LAG] La decisión del ADR-001 se mantiene, ahora con evidencia medida.

### Consecuencias / trade-offs
- Spark procesó entre 31 y 113 filas/s en las 5 queries, con los watermarks avanzando.
- Se conserva el orden de eventos por zona, necesario para las ventanas de Spark.
- El desbalance es conocido y predecible: p1 concentra 6 zonas.
- Si la demanda crece, se revisa de nuevo con la misma tabla de `resumen.py`.