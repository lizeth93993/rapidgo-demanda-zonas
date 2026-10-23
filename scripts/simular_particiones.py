"""Simula como se reparten las zonas entre particiones de Kafka.

Reproduce el particionador por defecto de Kafka (murmur2 sobre los bytes de la key,
`(hash & 0x7fffffff) % particiones`), que es el mismo que usara el producer si se
configura `partitioner=murmur2_random` en confluent-kafka (ver ADR-001).

Uso:
    python scripts/simular_particiones.py            # compara 3, 4, 6 y 12 particiones
    python scripts/simular_particiones.py 6          # detalle para 6 particiones

Evidencia usada en docs/decisions/ADR-001-topic-key-particiones.md.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def murmur2(data: bytes) -> int:
    """Port exacto de org.apache.kafka.common.utils.Utils.murmur2 (entero de 32 bits con signo)."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    mask = 0xFFFFFFFF
    h = (seed ^ length) & mask
    length4 = length // 4
    for i in range(length4):
        i4 = i * 4
        k = (data[i4] & 0xFF) | ((data[i4 + 1] & 0xFF) << 8) | ((data[i4 + 2] & 0xFF) << 16) | ((data[i4 + 3] & 0xFF) << 24)
        k = (k * m) & mask
        k ^= k >> r
        k = (k * m) & mask
        h = (h * m) & mask
        h ^= k
    rest = length % 4
    base = length & ~3
    if rest == 3:
        h ^= (data[base + 2] & 0xFF) << 16
    if rest >= 2:
        h ^= (data[base + 1] & 0xFF) << 8
    if rest >= 1:
        h ^= data[base] & 0xFF
        h = (h * m) & mask
    h ^= h >> 13
    h = (h * m) & mask
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def particion(key: str, n: int) -> int:
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % n


def cargar_zonas() -> dict[str, float]:
    cfg = yaml.safe_load((ROOT / "config" / "zonas.yaml").read_text(encoding="utf-8"))
    return {z["zone_id"]: float(z["baseline_opm"]) for z in cfg["zonas"]}


def resumen(zonas: dict[str, float], n: int) -> tuple[list[float], list[list[str]], float, int]:
    carga = [0.0] * n
    miembros: list[list[str]] = [[] for _ in range(n)]
    for zone_id, opm in zonas.items():
        p = particion(zone_id, n)
        carga[p] += opm
        miembros[p].append(zone_id)
    total = sum(carga)
    promedio = total / n
    sesgo = max(carga) / promedio  # 1.0 = reparto perfecto
    vacias = sum(1 for c in carga if c == 0)
    return carga, miembros, sesgo, vacias


def main() -> None:
    zonas = cargar_zonas()
    total = sum(zonas.values())
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
        carga, miembros, sesgo, vacias = resumen(zonas, n)
        print(f"{n} particiones | demanda base total {total:.0f} pedidos/min")
        for p in range(n):
            print(f"  p{p}: {carga[p]:5.1f} opm ({carga[p] / total:5.1%})  {', '.join(miembros[p]) or '-'}")
        print(f"  sesgo max/promedio = {sesgo:.2f} | particiones vacias = {vacias}")
        return
    print(f"{len(zonas)} zonas | demanda base total {total:.0f} pedidos/min (hora pico)")
    print("particiones | sesgo max/prom | vacias | carga max (opm)")
    for n in (3, 4, 6, 12):
        carga, _, sesgo, vacias = resumen(zonas, n)
        print(f"{n:>11} | {sesgo:>14.2f} | {vacias:>6} | {max(carga):>6.1f}")


if __name__ == "__main__":
    main()
