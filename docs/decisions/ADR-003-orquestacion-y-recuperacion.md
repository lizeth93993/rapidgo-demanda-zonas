# ADR-003 - Orquestación del control batch y recuperación ante fallos

- **Estado:** Aceptada (Semana 3, release v0.3.0-orchestration)
- **Issue:** #14 · **PR:** #32
- **Responsables:** Lizeth Jimenez (@lizeth93993), Santiago Peña (@tarantis-udec), Carlos Guzman (@anotheruserbtww)

## Contexto / problema

Spark Structured Streaming escribe el Data Lake de forma continua, pero nadie revisaba si los datos seguían llegando ni si eran confiables. Si el producer, Kafka o Spark se detienen, las alertas simplemente dejan de aparecer y no hay forma de distinguir "no hay picos" de "no hay datos". Además, el KPI "demanda por franja" no es de streaming: es un resumen por día y franja que tiene sentido calcular por lotes.

Necesitábamos decidir **cómo orquestar** esos controles y **cómo recuperarnos** cuando fallan.

## Opciones consideradas

| Opción | Ventaja | Desventaja |
|---|---|---|
| A. Controles dentro del job de Spark | Un solo proceso | Si Spark se cae, el control también; mezcla streaming con lógica batch |
| B. DAG de Airflow cada hora, con validaciones encadenadas, reintentos cortos y reejecución manual (*Clear*) | Detecta fallos de cualquier componente, deja historial visible y cada paso se puede repetir | Un servicio más en Docker; el fallo se detecta con hasta 1 h de retraso si nadie lo dispara a mano |
| C. Sensor de Airflow que espera datos nuevos | Espera en vez de fallar | Oculta el problema: una fuente caída parece una tarea "en espera" |

## Decisión tomada

**Opción B.** DAG `rapidgo_control_demanda` con 6 tareas:

1. `verificar_fuentes` falla si el último evento raw tiene más de **5 min** (fuente detenida).
2. `validar_raw` falla si en los últimos **5 min** hay menos de **100** eventos o más de **5 %** inválidos.
3. `agregar_demanda_franja` y `resumen_alertas` corren en paralelo.
4. `validar_curated` revisa el KPI.
5. `generar_reporte` junta las métricas que llegan por XCom.

Estrategia de recuperación:

- **2 reintentos cada 30 s** por tarea: absorben fallos momentáneos (un archivo que Spark está escribiendo, un reinicio corto de un contenedor) sin intervención.
- Si después de los reintentos sigue fallando, la tarea queda en **failed** y las siguientes en **upstream_failed**: no se genera un reporte con datos malos.
- **Recuperación:** se corrige la causa y se hace **Clear** en la tarea fallida, que vuelve a correr junto con todas las siguientes.
- Las tareas son **idempotentes**: `kpi_demanda_franja` se reescribe completo y cada reporte es un archivo nuevo, así que repetir una tarea no duplica datos.
- La validación mira una **ventana reciente** (5 min) y no todo el histórico, para que el DAG se recupere en cuanto los datos vuelven a estar bien.

## Evidencia utilizada

Fallo controlado, escenario `fuente detenida`, el 6 de noviembre de 2026 (horas locales, UTC-5; Airflow muestra UTC). Run manual `manual__2026-11-06T16:30:10.276324+00:00`.

| Paso | Hora | Qué pasó |
|---|---|---|
| Se provocó el fallo | 11:22:33 | `docker compose stop producer` y espera de más de 5 min |
| Trigger manual | 11:30:11 | Inicia el run manual |
| Primer intento | 11:30:11 → 11:30:20 | `verificar_fuentes failed` → `up_for_retry` ("hace 20.2 min") |
| Segundo intento | 11:30:51 → 11:31:01 | `failed` → `up_for_retry` ("hace 20.8 min") |
| Tercer intento | 11:31:32 → 11:31:41 | `failed`; las 5 tareas siguientes `upstream_failed`. Run fallido a las 11:31:44 |
| Se corrigió la causa | 11:38:24 | `docker compose start producer`; además se reinició Spark (ver "Hallazgo") hasta que volvió a ingerir datos frescos |
| Clear de la tarea | 12:28:30 | `verificar_fuentes` OK a las 12:28:46 (último evento hace 0.59 min); las 6 tareas en `success`, fin del run a las 12:33:32 |

Mensaje real del log de la tarea (intento 3):

```
rapidgo_calidad.FuenteNoDisponible: El ultimo evento raw se ingirio hace 21.5 min (limite 5 min). Revisar que el producer, Kafka y Spark esten corriendo (docker compose ps).
```

Reintentos observados: 3 intentos fallidos, con ~30 s entre el fin de un intento y el inicio del siguiente (lo configurado: `retries=2`, `retry_delay=30 s`). El intento 4 fue el del Clear.

**Hallazgo durante la prueba.** El mensaje decía 20-21 min sin datos, aunque el producer solo llevaba ~8 min detenido. Restando, el último evento raw entró hacia las 11:10, antes de detener el producer: Spark había salido con `Exited (1)` hacia las 11:11 por un `FileNotFoundException` en `datalake/curated/alertas/_temporary`, y volvió a caerse otras veces durante la sesión. Es decir, la fuente ya estaba caída por Spark cuando se provocó el fallo, y el DAG lo detectó igual: el control funciona sin importar qué componente se detenga. Para recuperar hubo que reiniciar el producer y Spark. Hipótesis (no confirmada): dos queries (`zonas_calientes` y `capacidad`) escriben en la misma ruta `curated/alertas` y se borran mutuamente los archivos temporales.

## Consecuencias / trade-offs

- (+) Un fallo de cualquier componente (producer, Kafka o Spark) se ve en Airflow con un mensaje que dice qué revisar.
- (+) No se generan reportes con datos incompletos o inválidos.
- (-) Con `@hourly`, un fallo puede tardar hasta una hora en detectarse si nadie dispara el DAG a mano. Lo dejamos en `@hourly`: en la prueba, una vez disparado, el DAG detectó la fuente caída en ~1.5 min y se recuperó con un solo Clear, y el KPI de demanda por franja es diario, así que correrlo más seguido no aporta. Para detectar caídas antes, la mejora propuesta es `restart: on-failure` en el servicio `spark` del compose, en vez de bajar el horario.
- (-) Los umbrales de 5 min y 5 % salen de nuestra simulación (con el producer normal la tasa de inválidos fue de 0.99 % en el run de antes del fallo y 1.01 % en el run recuperado). En producción habría que recalibrarlos.
