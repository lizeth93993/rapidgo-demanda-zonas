# Checkpoint Semana 1 - v0.1.0-foundation

**Equipo 2 · Variante 5: Demanda por zonas** · Lizeth Jimenez (@lizeth93993), Santiago Peña (@tarantis-udec), Carlos Guzman (@anotheruserbtww)

## 1. Que quedo listo
Repositorio con estructura base, contrato JSON del evento con schema y samples, catalogo de 12 zonas, 3 KPIs y 2 reglas de alerta con umbrales en `config/umbrales.yaml`, topic/key/particiones definidos (ADR-001), `docker-compose.yml` preliminar con Kafka KRaft + creacion del topic, Kanban con 17 Issues repartidos en las 4 semanas.

## 2. Decision mas importante
**Key = `zone_id` con 3 particiones** (ADR-001). La simulacion del hash de Kafka mostro que con 12 zonas, 6 o 12 particiones dejan el reparto mas desbalanceado (12 particiones dejan 5 vacias). Con 3 el sesgo es 1.51 y mantenemos el orden de los eventos de cada pedido, necesario para saber si un pedido sigue sin asignar.

## 3. Riesgo principal
**Particion caliente:** Usaquen y Suba comparten particion (~50 % de la demanda base). Un pico en esas zonas puede generar lag en Spark y retrasar justo las alertas que queremos disparar.
Mitigacion: en la Semana 2 medir mensajes por particion y lag durante una corrida con pico; si se confirma, pasar a asignacion explicita zona -> particion (ADR-002).

Riesgo secundario: los umbrales son supuestos de simulacion (presion normal ~0.7 pedidos por repartidor en 15 min). Se recalibran con los valores observados en la Semana 2.



## 4. Plan Semana 2 (v0.2.0-streaming)
| Integrante | Rol | Entrega |
|---|---|---|
| Santiago Peña (@tarantis-udec) | Kafka / Producer | Producer con picos simulados y key `zone_id` (#7) |
| Carlos Guzman (@anotheruserbtww) | Spark / Data Lake | Lectura, parseo, transformaciones, ventanas y alertas a Parquet con checkpoint (#8, #9) |
| Lizeth Jimenez (@lizeth93993) | Tests / CI | Pruebas de contrato y reglas, `ci.yml` con push, pull_request y workflow_dispatch (#10) |

## 5. Enlaces
- Issues de la semana: https://github.com/lizeth93993/rapidgo-demanda-zonas/milestone/1
- PRs:
  - #__ Estructura base (Issue #1)
  - #18 KPIs y reglas de alerta (Issue #3)
  - #__ docker-compose (Issue #5)
  - #__ Contrato JSON (Issue #2)
  - #__ ADR-001 Kafka (Issue #4)
- Release: https://github.com/lizeth93993/rapidgo-demanda-zonas/releases/tag/v0.1.0-foundation