# ADR-001 - Topic, key y numero de particiones de Kafka

- **Estado:** Aceptada (Semana 1, release v0.1.0-foundation)
- **Issue:** #4 · **PR:** #_(completar)_
- **Responsables:** Kevin Pena (autor), Lizeth Jimenez (revision)

## Contexto / problema

El pipeline necesita detectar picos de demanda **por zona** y comparar la demanda con los repartidores activos **de esa zona**. Cada pedido emite varios eventos (CREATED, ASSIGNED, PICKED_UP, DELIVERED / CANCELLED) que deben leerse en orden para saber si un pedido sigue sin asignar. Hay 12 zonas con demanda muy desigual: Chapinero espera 9 pedidos/min en hora pico y Ciudad Bolivar 1.5 (`config/zonas.yaml`). El entorno es local: 1 broker y Spark con 3-4 nucleos.

Hay que decidir el nombre del topic, la key de los mensajes y el numero de particiones.

## Opciones consideradas

| Opcion | Ventaja | Desventaja |
|---|---|---|
| A. key = `order_id` | Reparto casi uniforme entre particiones | El orden se garantiza por pedido, pero no por zona; un consumidor por zona tendria que leer todas las particiones |
| B. key = `zone_id`, hash por defecto (murmur2) | Todos los eventos de una zona (y por tanto de cada pedido) quedan en una particion y en orden | Con solo 12 keys el reparto puede quedar desbalanceado; una zona en pico carga su particion |
| C. key = `zone_id` con asignacion explicita zona -> particion | Reparto balanceado | Codigo y configuracion extra en el producer; cada zona nueva exige actualizar la tabla |
| D. Sin key (round robin) | Reparto perfecto | Se pierde el orden de los eventos del mismo pedido |

## Evidencia utilizada

Ejecutamos `python scripts/simular_particiones.py`, que reproduce el hash murmur2 de Kafka (verificado contra los vectores de prueba de Kafka en `tests/test_particiones.py`) con la demanda base de cada zona:

```
12 zonas | demanda base total 56 pedidos/min (hora pico)
particiones | sesgo max/prom | vacias | carga max (opm)
          3 |           1.51 |      0 |   28.0
          4 |           1.84 |      0 |   25.5
          6 |           2.22 |      0 |   20.5
         12 |           4.11 |      5 |   19.0
```

Hallazgos:
- Con 12 particiones, 5 quedan **vacias**: mas particiones no da mas paralelismo con solo 12 keys.
- Con 6 particiones, Usaquen, Suba, Barrios Unidos y Ciudad Bolivar caen en la misma particion (37 % de la demanda).
- Con 3 particiones el sesgo es el menor (1.51): la particion mas cargada lleva el 50 % de la demanda.
- La opcion C con 3 particiones bajaria el sesgo a 1.03 (asignacion greedy por demanda base).
- El volumen esperado es bajo (~56 pedidos/min ≈ 1 mensaje/s, x4 contando los cambios de estado), por lo que el throughput de una particion no es el cuello de botella.

## Decision tomada

- **Topic:** `rapidgo.demanda.pedidos.v1` (dominio.variante.entidad.version; un cambio incompatible de contrato crea `.v2`).
- **Key:** `zone_id` (opcion B).
- **Particiones:** **3**, replication factor 1 (un solo broker local).
- **Producer:** confluent-kafka usa por defecto un particionador CRC32 (`consistent_random`), distinto al de Kafka en Java. Configuramos `partitioner=murmur2_random` para que la asignacion real coincida con la simulacion y con las herramientas de Kafka.
- **Topic de alertas:** `rapidgo.demanda.alertas.v1` con 1 particion (volumen muy bajo, orden global de alertas).

Se eligen 3 y no mas porque las particiones pueden aumentarse pero no reducirse, y aumentarlas cambia la particion de cada key. Empezar en 3 deja margen para crecer si la evidencia lo pide.

## Consecuencias / trade-offs

- (+) Orden garantizado de los eventos de cada pedido y de cada zona.
- (+) Paralelismo de 3 consumidores, coherente con Spark local.
- (-) Desbalance conocido: la particion con Usaquen y Suba recibe ~50 % de la demanda. Si hay un pico en esas zonas, esa particion acumula mas lag.
- **Criterio para revisar (Semana 2):** medir los mensajes por particion con `kafka-get-offsets.sh` despues de una corrida con pico. Si la particion mas cargada supera 1.5 veces el promedio y genera lag visible en Spark, cambiar a la opcion C y registrarlo en ADR-002.
