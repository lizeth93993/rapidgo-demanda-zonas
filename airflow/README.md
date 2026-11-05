# Airflow (Issue #12)

DAG `rapidgo_control_demanda`: control batch del Data Lake que escribe Spark. Corre cada hora (`@hourly`, sin catchup) o a mano desde la UI.

```
verificar_fuentes -> validar_raw -> agregar_demanda_franja -> validar_curated -> generar_reporte
                               \-> resumen_alertas ----------------------------/
```

| Tarea | Qué hace | Falla si… |
|---|---|---|
| `verificar_fuentes` | Hay Parquet en `raw/pedidos` y el último evento llegó hace pocos minutos | No hay datos, o el último tiene más de `max_minutos_sin_datos` (5 min): producer, Kafka o Spark detenidos |
| `validar_raw` | Volumen y tasa de inválidos de los eventos de los últimos `ventana_validacion_min` (5 min) | Menos de 100 eventos o más de 5 % inválidos |
| `agregar_demanda_franja` | KPI demanda por franja → `curated/kpi_demanda_franja` (se reescribe: idempotente) | — |
| `resumen_alertas` | Cuenta las alertas de `curated/alertas` por tipo, severidad y zona | — |
| `validar_curated` | El KPI no está vacío, sin zonas ni franjas desconocidas, sin negativos | Alguna de esas reglas |
| `generar_reporte` | Escribe `datalake/reports/reporte_<fecha>.md` y `ultimo_reporte.md` | — |

- **Reintentos:** 2 por tarea, cada 30 s (`default_args`).
- **XCom:** cada tarea devuelve un diccionario de métricas que reciben las siguientes; `generar_reporte` los junta todos.
- **Reglas:** `rapidgo_calidad.py` (Python puro) — lo prueba `tests/test_calidad.py` en CI. Umbrales en la sección `calidad` de `config/umbrales.yaml`.

## Usarlo

```bash
docker compose up -d --build airflow     # la primera vez descarga la imagen (~1.5 GB)
```

1. Abrir **http://localhost:8081** (sin usuario ni contraseña).
2. **Dags** → `rapidgo_control_demanda` → activar el interruptor (al activarlo corre una vez sola).
3. Para correrlo a mano: botón **Trigger** (▶).
4. Ver cada tarea: clic en el cuadro de la tarea → **Logs**. El reporte queda en `datalake/reports/ultimo_reporte.md`.

## Fallo controlado

| Escenario | Cómo provocarlo | Tarea que falla | Cómo recuperar |
|---|---|---|---|
| Fuente detenida | `docker compose stop producer`, esperar 6 min y disparar el DAG | `verificar_fuentes` (tras 2 reintentos) | `docker compose start producer`, esperar 1 min y **Clear** la tarea |
| Datos inválidos | `docker compose stop producer` y `TASA_INVALIDOS=0.3 docker compose up -d producer`, esperar 2 min y disparar | `validar_raw` | Volver a `docker compose up -d producer` (sin la variable), esperar 6 min y **Clear** |
