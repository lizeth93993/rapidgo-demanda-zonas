# Producer (Issue #7)

Simulador de pedidos que publica en `rapidgo.demanda.pedidos.v1` siguiendo `samples/order_event.schema.json`.

| Archivo | Qué hace |
|---|---|
| `generador.py` | Lógica pura (sin Kafka): tasas por zona, picos, ciclo de vida del pedido, reloj simulado. Se prueba en `tests/test_producer.py`. |
| `producer.py` | CLI: envía los eventos a Kafka con key `zone_id` y `partitioner=murmur2_random` (ADR-001). |
| `Dockerfile` | Imagen del servicio `producer` del docker-compose. |

**Reloj simulado:** con `--velocidad 10` (por defecto) cada segundo real avanza 10 s de `event_ts`. Las ventanas de 5 y 15 min de Spark se llenan en 30 s y 90 s reales.

```bash
# Sin Kafka: imprime 20 eventos
python producer/producer.py --dry-run --max-eventos 20

# Contra el Kafka del compose, desde el host
python producer/producer.py --bootstrap localhost:9092 --duracion 300

# Pico x2.5 en Chapinero desde el minuto simulado 5 y Suba con la mitad de repartidores
python producer/producer.py --pico BOG-CHA --factor 2.5 --reducir-repartidores BOG-SUB
```

Cada 10 s imprime cuántos mensajes confirmó Kafka **por partición**: es la evidencia para revisar el desbalance previsto en el ADR-001.
