# RapidGo · Variante 5 — Demanda por zonas

Pipeline Big Data end-to-end (Kafka → Spark Structured Streaming → Data Lake → Airflow) para detectar **picos de solicitudes por zona que afectan la disponibilidad y el servicio** de RapidGo.

**Equipo 2** · Línea de Profundización I - Big Data · Ingeniería de Sistemas y Computación, UCundinamarca Facatativá

| Integrante | GitHub | Rol Semana 2 | Rol Semana 3 | Rol Semana 4 |
|---|---|---|---|---|
| [Santiago Peña] | @tarantis-udec | Kafka / Producer | Airflow | Integración |
| [Carlos Guzman] | @anotheruserbtww | Spark / Data Lake | Tests / CI | Release |
| [Lizeth Jimenez] | @lizeth93993 | Tests / CI | Integración | Calidad / README |

**Estado:** `v0.2.0-streaming` — pipeline Kafka → Spark → Data Lake funcionando, con CI en cada PR.

[![CI RapidGo](https://github.com/lizeth93993/rapidgo-demanda-zonas/actions/workflows/ci.yml/badge.svg)](https://github.com/lizeth93993/rapidgo-demanda-zonas/actions/workflows/ci.yml)

## Problema de negocio

En horas de almuerzo y cena, o por eventos locales (lluvia, partidos, conciertos), algunas zonas reciben muchos más pedidos de los que sus repartidores pueden atender. Si nadie lo detecta a tiempo, los pedidos esperan sin repartidor, suben los tiempos de entrega y aumentan las cancelaciones.

**Valor de la solución:** medir la demanda por zona minuto a minuto, señalar las zonas calientes y alertar cuando la demanda supera la capacidad, para que operaciones pueda mover repartidores, activar incentivos o ajustar tiempos prometidos antes de que el servicio se degrade.

## Arquitectura

![Arquitectura](docs/arquitectura.png)

| Componente | Rol en la variante |
|---|---|
| Producer (Python, confluent-kafka) | Simula pedidos en 12 zonas de Bogotá con picos por zona y franja |
| Kafka (KRaft) | Transporta eventos; key `zone_id`, 3 particiones ([ADR-001](docs/decisions/ADR-001-topic-key-particiones.md)) |
| Spark Structured Streaming | Valida, limpia, calcula franja, agrega por ventanas y evalúa alertas |
| Data Lake (Parquet) | Capas raw → processed → curated ([detalle](datalake/README.md)) |
| Airflow | Control batch: calidad → agregación por franja → reporte |
| Docker Compose | Levanta todo el entorno local |
| GitHub (Issues, Kanban, PR, Actions, Releases) | Trazabilidad y CI desde la Semana 2 |

## Kafka

| Topic | Key | Particiones | Uso |
|---|---|---|---|
| `rapidgo.demanda.pedidos.v1` | `zone_id` | 3 | Eventos de pedido (un evento por cambio de estado) |
| `rapidgo.demanda.alertas.v1` | `zone_id` | 1 | Alertas generadas por Spark |

Justificación y evidencia en [ADR-001](docs/decisions/ADR-001-topic-key-particiones.md). Para reproducirla: `python scripts/simular_particiones.py`.

## Contrato del evento

Schema: [`samples/order_event.schema.json`](samples/order_event.schema.json) · Ejemplo: [`samples/order_event.sample.json`](samples/order_event.sample.json) · Ciclo de vida completo: [`samples/order_events.sample.jsonl`](samples/order_events.sample.jsonl)

```json
{
  "event_id": "6f1c2a4e-8b3d-4c7a-9e21-5d0f3b8a7c14",
  "schema_version": "1.0",
  "team_id": "equipo-02",
  "order_id": "ORD-20261022-K7M2QX",
  "zone_id": "BOG-CHA",
  "category": "RESTAURANTE",
  "status": "CREATED",
  "event_ts": "2026-10-22T17:31:05.412Z",
  "active_couriers_in_zone": 184,
  "channel": "APP"
}
```

| Campo | Tipo | Notas |
|---|---|---|
| `event_id` | UUID | Deduplicación en Spark |
| `schema_version` | `"1.0"` | Cambio incompatible ⇒ topic `.v2` |
| `team_id` | `"equipo-02"` | Identifica los eventos del equipo |
| `order_id` | string | Igual en todos los eventos del pedido |
| `zone_id` | enum (12) | Key de Kafka; catálogo en [`config/zonas.yaml`](config/zonas.yaml) |
| `category` | enum | RESTAURANTE, MERCADO, FARMACIA, LICORES, MASCOTAS, OTROS |
| `status` | enum | CREATED, ASSIGNED, PICKED_UP, DELIVERED, CANCELLED |
| `event_ts` | ISO-8601 UTC | Event time para ventanas; la franja se calcula en America/Bogota |
| `active_couriers_in_zone` | int ≥ 0 | Repartidores activos en la zona según despacho; base para la regla de capacidad |
| `channel` | enum, opcional | APP, WEB, CALL_CENTER |

Decisión de diseño: en lugar de un segundo topic de disponibilidad de repartidores, el sistema de despacho adjunta `active_couriers_in_zone` a cada evento. Así la regla de capacidad se calcula con un solo stream, sin joins entre streams.

## KPIs y reglas de alerta

Umbrales en [`config/umbrales.yaml`](config/umbrales.yaml); demanda normal por zona (`baseline_opm`) en [`config/zonas.yaml`](config/zonas.yaml).

### KPIs

| KPI | Cálculo | Ventana |
|---|---|---|
| **Pedidos por minuto (opm) por zona** | `count(status = CREATED)` por `zone_id` | Tumbling 1 min, watermark 2 min |
| **Zonas calientes** | Índice de calor = opm de la ventana / `baseline_opm` de la zona; caliente si ≥ 1.5, más el top 3 por opm | Deslizante 5 min cada 1 min |
| **Demanda por franja** | Pedidos por fecha, franja (madrugada, desayuno, almuerzo, tarde, cena, noche) y zona | Diaria (Airflow) |

### Alertas

| Alerta | Regla | WARNING | CRITICAL |
|---|---|---|---|
| **DEMANDA_ALTA** | Índice de calor en ventana de 5 min (cada 1 min), solo si opm ≥ 3 | ≥ 1.5 | ≥ 2.0 |
| **CAPACIDAD_INSUFICIENTE** | Presión = pedidos CREATED en 15 min / mínimo de `active_couriers_in_zone` en la ventana (cada 5 min) | ≥ 1.0 | ≥ 1.5, o 0 repartidores con pedidos |

### Justificación de los umbrales iniciales

- **Índice de calor 1.5 / 2.0:** se compara cada zona con su propia demanda normal. Un umbral absoluto de pedidos por minuto alertaría siempre en Chapinero (9 opm) y nunca en Ciudad Bolívar (1.5 opm). 1.5× es un pico que ya exige mover repartidores; 2.0× duplica la demanda y compromete los tiempos prometidos.
- **Mínimo de 3 opm:** en zonas de baja demanda, 2 o 3 pedidos sueltos multiplican el índice sin ser un problema real; el mínimo evita falsas alarmas.
- **Presión 1.0 / 1.5:** suponemos que un repartidor completa un ciclo en ~30 min y lleva hasta 2 pedidos, es decir, absorbe ~1 pedido nuevo cada 15 min. Presión 1.0 = la demanda iguala la capacidad; 1.5 = sobra un 50 % de pedidos que quedarán esperando. Los repartidores base de cada zona se dimensionaron para una presión normal de ~0.7, de modo que un pico de 1.5× en demanda lleva la presión a ~1.05 y dispara WARNING en ambas reglas de forma coherente.
- **Ventanas:** 5 min para demanda (reacciona rápido sin ruido de un solo minuto); 15 min para capacidad (coincide con el horizonte de absorción de un repartidor).
- **Watermark 2 min:** tolera eventos que llegan con retraso desde la app móvil sin retener demasiado estado.

Todos son **supuestos de simulación**. En la Semana 2 se compararán con los valores observados en la ejecución y cualquier ajuste se hará por PR con su evidencia.

## Ejecutar el pipeline

Prerrequisitos: Docker Desktop (con al menos 4 GB de memoria asignada), Python 3.13 y Git.

```bash
git clone https://github.com/lizeth93993/rapidgo-demanda-zonas.git && cd rapidgo-demanda-zonas

# 1. Levantar todo: Kafka, topics, Kafka UI, producer y Spark
docker compose up -d --build
#    La primera vez tarda: descarga las imágenes y el conector de Kafka para Spark.

# 2. Ver que funciona
docker compose logs -f producer      # [resumen] con mensajes confirmados por partición
docker compose logs -f spark         # [progreso] por query y [ALERTA] cuando se dispara una regla
#    Kafka UI: http://localhost:8080 (topic rapidgo.demanda.pedidos.v1, 3 particiones)

# 3. Provocar un pico en Chapinero y pocos repartidores en Suba
docker compose stop producer
PICO_ZONA=BOG-CHA REDUCIR_ZONA=BOG-SUB docker compose up -d producer
#    El pico empieza en el minuto simulado 5 (30 s reales) y dura 20 (2 min reales).

# 4. Ver la evidencia del Data Lake
docker compose exec spark /opt/spark/bin/spark-submit /app/spark/resumen.py

# 5. Apagar
docker compose down
```

| Variable del producer | Default | Qué hace |
|---|---|---|
| `VELOCIDAD` | 10 | Segundos simulados por segundo real. |
| `PICO_ZONA` / `PICO_FACTOR` | – / 2.5 | Zona con pico de demanda y multiplicador. |
| `REDUCIR_ZONA` | – | Zona que pierde la mitad de sus repartidores durante el pico. |
| `DURACION` | 0 | Segundos reales a ejecutar (0 = sin límite). |

`docker compose down` conserva los mensajes de Kafka (volumen `kafka-data`) y el Data Lake con sus checkpoints, así que al volver a levantar todo continúa donde iba. El producer retoma su reloj simulado desde el último evento del topic.

Para empezar de cero: `docker compose down -v` (borra los volúmenes) y luego borrar el contenido de `datalake/raw`, `datalake/processed`, `datalake/curated` y `datalake/_checkpoints` (sin borrar los `.gitkeep`). Hay que hacer las dos cosas juntas: si se borra solo una, los checkpoints de Spark y Kafka quedan desincronizados.

## Pruebas y CI

```bash
python -m venv .venv && source .venv/Scripts/activate   # Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
pytest -v                                               # contrato, particiones, reglas y producer
python producer/producer.py --dry-run --max-eventos 20  # producer sin Kafka
```

GitHub Actions (`.github/workflows/ci.yml`) corre en cada push a `main`, en cada Pull Request y manualmente (**Actions → CI RapidGo → Run workflow**): compileall, validación de YAML, `pytest`, dry-run del producer y `docker compose config`.

## Estructura

```
.github/workflows/   ci.yml (GitHub Actions)
airflow/dags/        DAG de control (Semana 3)
config/              zonas.yaml, umbrales.yaml
datalake/            raw / processed / curated (+ README)
docs/                arquitectura.png, checkpoints/, decisions/ (ADR)
producer/            simulador de pedidos (generador.py + producer.py + Dockerfile)
samples/             schema y eventos de ejemplo
scripts/             simulación de particiones, creación de issues
spark/               streaming_job.py, reglas.py, resumen.py, Dockerfile
tests/               pruebas de contrato, particiones, reglas y producer
```

## Documentación

- [ADR-001 — Topic, key y particiones](docs/decisions/ADR-001-topic-key-particiones.md)
- [ADR-002 — Ajuste tras observar la ejecución](docs/decisions/ADR-002-ajuste-tras-ejecucion.md)
- [Checkpoint Semana 1](docs/checkpoints/semana1.md)
- [Checkpoint Semana 2](docs/checkpoints/semana2.md)

## Releases

| Versión | Contenido | Estado |
|---|---|---|
| v0.1.0-foundation | Arquitectura, contrato, KPIs, alertas, ADR-001, Kanban | ✅ |
| v0.2.0-streaming | Producer, Kafka, Spark, Data Lake, tests, CI | ✅ |
| v0.3.0-orchestration | Airflow, calidad, fallo controlado, CI ampliado | ⏳ |
| v1.0.0 | Quality gate, README final, video | ⏳ |
