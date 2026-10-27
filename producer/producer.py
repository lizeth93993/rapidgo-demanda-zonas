"""Producer de RapidGo - Variante 5: Demanda por zonas.

Publica en Kafka el ciclo de vida de los pedidos con key = zone_id (ADR-001).

Ejemplos:
  # Prueba sin Kafka: imprime 20 eventos JSON y termina
  python producer/producer.py --dry-run --max-eventos 20

  # Trafico normal contra el Kafka del docker-compose (desde el host)
  python producer/producer.py --bootstrap localhost:9092

  # Pico de demanda x2.5 en Chapinero a partir del minuto simulado 5
  python producer/producer.py --pico BOG-CHA --factor 2.5

  # Ademas, Suba con la mitad de repartidores durante la misma ventana
  python producer/producer.py --pico BOG-CHA --reducir-repartidores BOG-SUB
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generador import Escenario, Generador, cargar_zonas  # noqa: E402

TOPIC_POR_DEFECTO = "rapidgo.demanda.pedidos.v1"


def parsear_argumentos(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulador de pedidos RapidGo (variante 5)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", TOPIC_POR_DEFECTO))
    p.add_argument("--zonas", default=os.getenv("ZONAS_PATH"), help="ruta a config/zonas.yaml")
    p.add_argument("--velocidad", type=float, default=float(os.getenv("VELOCIDAD", "10")),
                   help="segundos simulados por segundo real (default 10)")
    p.add_argument("--duracion", type=float, default=float(os.getenv("DURACION", "0")),
                   help="segundos reales a ejecutar; 0 = sin limite")
    p.add_argument("--max-eventos", type=int, default=0, help="detenerse tras N eventos; 0 = sin limite")
    p.add_argument("--pico", default=os.getenv("PICO_ZONA") or None, metavar="ZONA")
    p.add_argument("--factor", type=float, default=float(os.getenv("PICO_FACTOR", "2.0")))
    p.add_argument("--pico-inicio", type=float, default=float(os.getenv("PICO_INICIO", "5")),
                   help="minuto simulado en que empieza el pico")
    p.add_argument("--pico-duracion", type=float, default=float(os.getenv("PICO_DURACION", "20")))
    p.add_argument("--reducir-repartidores", default=os.getenv("REDUCIR_ZONA") or None, metavar="ZONA")
    p.add_argument("--factor-repartidores", type=float, default=float(os.getenv("REDUCIR_FACTOR", "0.5")))
    p.add_argument("--tasa-invalidos", type=float, default=float(os.getenv("TASA_INVALIDOS", "0.01")))
    p.add_argument("--respetar-franja", action="store_true", help="escalar la demanda por la hora real de Bogota")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="imprimir eventos en vez de enviarlos a Kafka")
    p.add_argument("--resumen-cada", type=float, default=10.0, help="segundos reales entre resumenes")
    return p.parse_args(argv)


def crear_producer(bootstrap: str):
    from confluent_kafka import Producer  # import tardio: --dry-run no necesita Kafka
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "rapidgo-producer-equipo-02",
        "partitioner": "murmur2_random",   # mismo hash que Kafka en Java (ADR-001)
        "acks": "all",
        "linger.ms": 50,
        "enable.idempotence": True,
    })


def ultimo_event_ts(bootstrap: str, topic: str, timeout: float = 10.0):
    """Mayor timestamp entre los ultimos mensajes de cada particion del topic (None si esta vacio).

    El reloj simulado va adelantado respecto al reloj real. Si el producer se reinicia
    (por ejemplo, para activar un pico) debe continuar desde donde iba; si empezara otra
    vez en la hora actual, Spark descartaria esos eventos por llegar despues del watermark.
    """
    from confluent_kafka import Consumer, TopicPartition
    consumidor = Consumer({"bootstrap.servers": bootstrap, "group.id": "rapidgo-producer-reloj",
                           "enable.auto.commit": False})
    try:
        tema = consumidor.list_topics(topic, timeout=timeout).topics.get(topic)
        if tema is None or tema.error is not None:
            return None
        maximo = 0
        for particion in tema.partitions:
            bajo, alto = consumidor.get_watermark_offsets(TopicPartition(topic, particion), timeout=timeout)
            if alto <= bajo:
                continue
            consumidor.assign([TopicPartition(topic, particion, alto - 1)])
            msg = consumidor.poll(timeout)
            if msg is not None and msg.error() is None:
                maximo = max(maximo, msg.timestamp()[1])
        return datetime.fromtimestamp(maximo / 1000, timezone.utc) if maximo > 0 else None
    finally:
        consumidor.close()


def epoch_ms(event_ts: str) -> int:
    return int(datetime.strptime(event_ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


def main(argv=None) -> int:
    args = parsear_argumentos(argv)
    escenario = Escenario(
        velocidad=args.velocidad,
        pico_zona=args.pico, pico_factor=args.factor,
        pico_inicio_min=args.pico_inicio, pico_duracion_min=args.pico_duracion,
        reducir_zona=args.reducir_repartidores, reducir_factor=args.factor_repartidores,
        tasa_invalidos=args.tasa_invalidos, respetar_franja=args.respetar_franja,
    )
    inicio = datetime.now(timezone.utc)
    if not args.dry_run:
        try:
            ultimo = ultimo_event_ts(args.bootstrap, args.topic)
        except Exception as exc:  # sin Kafka todavia: se empieza en la hora actual
            print(f"[WARN] no se pudo leer el ultimo evento del topic: {exc}", flush=True)
            ultimo = None
        if ultimo and ultimo > inicio:
            inicio = ultimo
            print(f"Reloj simulado: continua desde el ultimo evento del topic ({inicio:%Y-%m-%d %H:%M:%S} UTC)",
                  flush=True)
    gen = Generador(cargar_zonas(args.zonas), escenario, inicio=inicio, seed=args.seed)

    producer = None if args.dry_run else crear_producer(args.bootstrap)
    por_particion: Counter = Counter()
    por_zona: Counter = Counter()
    errores = [0]

    def al_entregar(err, msg):
        if err is not None:
            errores[0] += 1
            print(f"[ERROR] entrega fallida: {err}", file=sys.stderr)
        else:
            por_particion[msg.partition()] += 1

    detener = [False]
    signal.signal(signal.SIGINT, lambda *_: detener.__setitem__(0, True))
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: detener.__setitem__(0, True))

    if not args.dry_run:
        print(f"Producer -> {args.bootstrap} topic={args.topic} velocidad=x{args.velocidad:g} "
              f"pico={args.pico or '-'} x{args.factor:g} reducir={args.reducir_repartidores or '-'}", flush=True)

    tick_real = 1.0
    enviados = 0
    t0 = ultimo_resumen = time.monotonic()
    while not detener[0]:
        lote = gen.avanzar(tick_real * args.velocidad)
        for ev in lote:
            payload = json.dumps(ev, ensure_ascii=False)
            if args.dry_run:
                print(payload)
            else:
                producer.produce(args.topic, key=ev.get("zone_id", "SIN-ZONA"), value=payload,
                                 timestamp=epoch_ms(ev["event_ts"]), on_delivery=al_entregar)
                producer.poll(0)
            por_zona[ev.get("zone_id", "SIN-ZONA")] += 1
            enviados += 1
            if args.max_eventos and enviados >= args.max_eventos:
                detener[0] = True
                break
        ahora = time.monotonic()
        if args.duracion and ahora - t0 >= args.duracion:
            break
        if detener[0]:
            break
        if not args.dry_run and ahora - ultimo_resumen >= args.resumen_cada:
            ultimo_resumen = ahora
            imprimir_resumen(gen, enviados, por_particion, errores[0])
        if not args.dry_run:
            time.sleep(max(0.0, tick_real - (time.monotonic() - ahora)))

    if producer is not None:
        producer.flush(15)
        imprimir_resumen(gen, enviados, por_particion, errores[0], final=True)
        print("Eventos por zona:", dict(sorted(por_zona.items())), flush=True)
    return 0


def imprimir_resumen(gen: Generador, enviados: int, por_particion: Counter, errores: int, final=False) -> None:
    total = sum(por_particion.values()) or 1
    reparto = " ".join(f"p{p}={n} ({n / total:.0%})" for p, n in sorted(por_particion.items()))
    etiqueta = "FINAL" if final else "resumen"
    print(f"[{etiqueta}] minuto simulado {gen.minuto_simulado():6.1f} | enviados={enviados} "
          f"| confirmados por particion: {reparto or '-'} | errores={errores}"
          f"{' | PICO ACTIVO' if gen.en_pico() and gen.esc.pico_zona else ''}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
