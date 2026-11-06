# Checkpoint Semana 3 - v0.3.0-orchestration

**Equipo 2 · Variante 5: Demanda por zonas** · Lizeth Jimenez, Santiago Peña, Carlos Guzman

## 1. Qué quedó funcionando
- DAG `rapidgo_control_demanda` en Airflow 3.1.8 (http://localhost:8081) con 6 tareas, dependencias, 2 reintentos por tarea y métricas por XCom.
- Validación de fuentes y de calidad de raw y curated, KPI demanda por franja en `curated/kpi_demanda_franja` y reporte en `datalake/reports/`.
- CI ampliado: un job nuevo importa el DAG con Airflow y corre las pruebas de calidad (97 pruebas en total).

## 2. Valor real observado
- Run de antes del fallo (6 nov 2026): 6 tareas en verde en 2 min 57 s; 19 999 eventos en los últimos 5 min, 198 inválidos, tasa 0.99 %.
- Run recuperado del 6 nov 2026, 12:33 (reporte `datalake/reports/ultimo_reporte.md`): 9 750 eventos en los últimos 5 min, tasa de inválidos 1.01 % (98 eventos), 485 878 eventos raw en total, 144 filas en `kpi_demanda_franja` (12 zonas).
- Zona con más pedidos: BOG-CHA (20 223 pedidos de 125 736 creados; le siguen BOG-USA con 18 093 y BOG-SUB con 17 103). Alertas registradas: 503 (376 DEMANDA_ALTA WARNING, 57 DEMANDA_ALTA CRITICAL y 70 CAPACIDAD_INSUFICIENTE WARNING); zonas con más alertas críticas: BOG-CAN (32) y BOG-CBO (17).

## 3. Fallo controlado
- **Tarea que falló:** `verificar_fuentes`
- **Cómo se provocó:** `docker compose stop producer` a las 11:22:33 y Trigger manual a las 11:30:11. Spark además estaba caído desde ~11:11 (`FileNotFoundException` en `curated/alertas/_temporary`), por eso el log dice ~21 min.
- **Mensaje real del log:**
  ```
  rapidgo_calidad.FuenteNoDisponible: El ultimo evento raw se ingirio hace 21.5 min (limite 5 min). Revisar que el producer, Kafka y Spark esten corriendo (docker compose ps).
  ```
- **Tareas afectadas:** 3 intentos de `verificar_fuentes` (11:30:11, 11:30:51 y 11:31:32, ~30 s entre cada uno); `validar_raw`, `agregar_demanda_franja`, `resumen_alertas`, `validar_curated` y `generar_reporte` en `upstream_failed`.
- **Cambio que lo resolvió:** `docker compose start producer` (11:38:24) y reinicio de Spark hasta tener datos frescos; Clear de `verificar_fuentes` a las 12:28:30 → run en `success` a las 12:33:32.

## 4. Decisión técnica
Orquestamos los controles con un DAG de Airflow cada hora, con validaciones encadenadas, 2 reintentos de 30 s y recuperación manual con Clear; las tareas son idempotentes y validan solo una ventana reciente de 5 min, así que el DAG se recupera en cuanto vuelven los datos. Lo dejamos en `@hourly` y proponemos `restart: on-failure` para Spark. Detalle en [ADR-003](../decisions/ADR-003-orquestacion-y-recuperacion.md).

## 5. Enlaces
- Issues: https://github.com/lizeth93993/rapidgo-demanda-zonas/milestone/3
- PRs: #30 DAG de Airflow · #31 pruebas y CI ampliado · #32 fallo controlado, ADR-003 y checkpoint
- Action con el job "Validar DAG de Airflow" en verde: https://github.com/lizeth93993/rapidgo-demanda-zonas/actions/runs/42152894165
- Release: https://github.com/lizeth93993/rapidgo-demanda-zonas/releases/tag/v0.3.0-orchestration
