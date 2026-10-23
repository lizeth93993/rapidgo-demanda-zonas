# Producer (Semana 2 - Issue #7)

Simulador de pedidos que publica en `rapidgo.demanda.pedidos.v1` siguiendo `samples/order_event.schema.json`.

Requisitos de diseno (Semana 1):
- Key del mensaje = `zone_id`; `partitioner=murmur2_random` en confluent-kafka (ADR-001).
- Tasa base por zona = `baseline_opm` de `config/zonas.yaml` x factor de franja horaria.
- Modo `--pico <zone_id> --factor 2.0` para provocar demanda alta y validar alertas.
- Modo `--reducir-repartidores <zone_id>` para provocar capacidad insuficiente.
- Cada pedido emite su ciclo de vida: CREATED -> ASSIGNED -> PICKED_UP -> DELIVERED (o CANCELLED).
