"""Generador de eventos de pedidos de RapidGo (variante 5) - logica pura, sin Kafka.

Usa un RELOJ SIMULADO: con velocidad=10, cada segundo real avanza 10 segundos de
event_ts. Asi las ventanas de 5 y 15 minutos de Spark se llenan en 30 s y 90 s
reales, lo que permite ver alertas en una demo corta. Las tasas se expresan en
pedidos por minuto de tiempo simulado, igual que baseline_opm.

Cada pedido emite su ciclo de vida: CREATED -> ASSIGNED -> PICKED_UP -> DELIVERED
(o CREATED -> CANCELLED), con retrasos en tiempo simulado.
"""
from __future__ import annotations

import heapq
import random
import string
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
TEAM_ID = "equipo-02"
SCHEMA_VERSION = "1.0"

CATEGORIAS = ["RESTAURANTE", "MERCADO", "FARMACIA", "LICORES", "MASCOTAS", "OTROS"]
PESOS_CATEGORIA = [0.55, 0.20, 0.08, 0.05, 0.04, 0.08]
CANALES = ["APP", "WEB", "CALL_CENTER"]
PESOS_CANAL = [0.80, 0.15, 0.05]

# Factor de demanda por franja (hora local Bogota) si se usa --respetar-franja.
FACTOR_FRANJA = {"MADRUGADA": 0.1, "DESAYUNO": 0.4, "ALMUERZO": 1.0, "TARDE": 0.5, "CENA": 1.0, "NOCHE": 0.3}

# Retrasos del ciclo de vida en segundos simulados (min, max).
RETRASO_ASIGNACION = (30, 150)
RETRASO_RECOGIDA = (240, 720)
RETRASO_ENTREGA = (480, 1500)
PROB_CANCELACION = 0.05


def cargar_zonas(ruta: Optional[Path] = None) -> list[dict]:
    ruta = Path(ruta) if ruta else ROOT / "config" / "zonas.yaml"
    return yaml.safe_load(ruta.read_text(encoding="utf-8"))["zonas"]


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@dataclass
class Escenario:
    """Parametros de la simulacion (los llena el CLI)."""
    velocidad: float = 10.0                      # segundos simulados por segundo real
    pico_zona: Optional[str] = None              # zona con pico de demanda
    pico_factor: float = 2.0                     # multiplicador de demanda en el pico
    pico_inicio_min: float = 5.0                 # minuto simulado en que empieza el pico
    pico_duracion_min: float = 20.0              # duracion del pico en minutos simulados
    reducir_zona: Optional[str] = None           # zona con menos repartidores
    reducir_factor: float = 0.5                  # fraccion de repartidores que quedan
    tasa_invalidos: float = 0.01                 # fraccion de eventos invalidos a proposito
    respetar_franja: bool = False                # escalar la demanda por franja horaria real
    factor_global: float = 1.0                   # multiplicador para toda la ciudad


@dataclass(order=True)
class _Pendiente:
    ts: datetime
    seq: int
    evento: dict = field(compare=False)


class Generador:
    def __init__(self, zonas: list[dict], escenario: Escenario, inicio: Optional[datetime] = None,
                 seed: Optional[int] = None):
        self.zonas = {z["zone_id"]: z for z in zonas}
        self.esc = escenario
        self.rng = random.Random(seed)
        self.inicio = inicio or datetime.now(timezone.utc)
        self.ahora = self.inicio
        self._cola: list[_Pendiente] = []
        self._seq = 0
        self._repartidores = {zid: float(z["repartidores_base"]) for zid, z in self.zonas.items()}
        for nombre in (escenario.pico_zona, escenario.reducir_zona):
            if nombre and nombre not in self.zonas:
                raise ValueError(f"zona desconocida: {nombre}")

    # ---------------- tasas ---------------- #
    def minuto_simulado(self) -> float:
        return (self.ahora - self.inicio).total_seconds() / 60

    def en_pico(self) -> bool:
        m = self.minuto_simulado()
        return self.esc.pico_inicio_min <= m < self.esc.pico_inicio_min + self.esc.pico_duracion_min

    def tasa_opm(self, zone_id: str) -> float:
        """Pedidos CREATED por minuto simulado que debe generar la zona ahora."""
        tasa = self.zonas[zone_id]["baseline_opm"] * self.esc.factor_global
        if self.esc.respetar_franja:
            hora_bog = (self.ahora - timedelta(hours=5)).hour
            tasa *= FACTOR_FRANJA[_franja(hora_bog)]
        if zone_id == self.esc.pico_zona and self.en_pico():
            tasa *= self.esc.pico_factor
        return tasa

    def repartidores(self, zone_id: str) -> int:
        """Paseo aleatorio suave alrededor de la base (+-5 %), con reduccion opcional."""
        base = self.zonas[zone_id]["repartidores_base"]
        actual = self._repartidores[zone_id] + self.rng.uniform(-1, 1)
        actual = min(max(actual, base * 0.95), base * 1.05)
        self._repartidores[zone_id] = actual
        if zone_id == self.esc.reducir_zona and self.en_pico():
            actual *= self.esc.reducir_factor
        return max(0, round(actual))

    # ---------------- eventos ---------------- #
    def _order_id(self) -> str:
        sufijo = "".join(self.rng.choices(string.ascii_uppercase + string.digits, k=6))
        return f"ORD-{self.ahora:%Y%m%d}-{sufijo}"

    def _evento(self, base: dict, status: str, ts: datetime) -> dict:
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "schema_version": SCHEMA_VERSION,
            "team_id": TEAM_ID,
            "order_id": base["order_id"],
            "zone_id": base["zone_id"],
            "category": base["category"],
            "status": status,
            "event_ts": iso_utc(ts),
            "active_couriers_in_zone": self.repartidores(base["zone_id"]),
            **({"channel": base["channel"]} if base.get("channel") else {}),
        }

    def _programar(self, ts: datetime, base: dict, status: str) -> None:
        self._seq += 1
        heapq.heappush(self._cola, _Pendiente(ts, self._seq, {"base": base, "status": status}))

    def _nuevo_pedido(self, zone_id: str, ts: datetime) -> dict:
        base = {
            "order_id": self._order_id(),
            "zone_id": zone_id,
            "category": self.rng.choices(CATEGORIAS, PESOS_CATEGORIA)[0],
            "channel": self.rng.choices(CANALES, PESOS_CANAL)[0] if self.rng.random() > 0.1 else None,
        }
        s = lambda r: timedelta(seconds=self.rng.uniform(*r))  # noqa: E731
        if self.rng.random() < PROB_CANCELACION:
            self._programar(ts + s(RETRASO_ASIGNACION), base, "CANCELLED")
        else:
            t_asig = ts + s(RETRASO_ASIGNACION)
            t_rec = t_asig + s(RETRASO_RECOGIDA)
            self._programar(t_asig, base, "ASSIGNED")
            self._programar(t_rec, base, "PICKED_UP")
            self._programar(t_rec + s(RETRASO_ENTREGA), base, "DELIVERED")
        return self._evento(base, "CREATED", ts)

    def _invalido(self, evento: dict) -> dict:
        """Rompe el evento a proposito para probar la validacion de Spark."""
        malo = dict(evento)
        tipo = self.rng.choice(["zona", "estado", "repartidores", "sin_zona"])
        if tipo == "zona":
            malo["zone_id"] = "BOG-XXX"
        elif tipo == "estado":
            malo["status"] = "PENDING"
        elif tipo == "repartidores":
            malo["active_couriers_in_zone"] = -1
        else:
            malo.pop("zone_id")
        return malo

    def avanzar(self, segundos_simulados: float) -> list[dict]:
        """Avanza el reloj y devuelve los eventos ocurridos en ese intervalo, en orden de event_ts."""
        desde = self.ahora
        hasta = desde + timedelta(seconds=segundos_simulados)
        minutos = segundos_simulados / 60
        for zone_id in self.zonas:
            self.ahora = desde
            n = _poisson(self.rng, self.tasa_opm(zone_id) * minutos)
            for _ in range(n):
                ts = desde + timedelta(seconds=self.rng.uniform(0, segundos_simulados))
                self._seq += 1
                heapq.heappush(self._cola, _Pendiente(ts, self._seq, {"nuevo": zone_id}))
        self.ahora = hasta
        salida = []
        while self._cola and self._cola[0].ts <= hasta:
            p = heapq.heappop(self._cola)
            self.ahora = p.ts
            if "nuevo" in p.evento:
                ev = self._nuevo_pedido(p.evento["nuevo"], p.ts)
            else:
                ev = self._evento(p.evento["base"], p.evento["status"], p.ts)
            if self.rng.random() < self.esc.tasa_invalidos:
                ev = self._invalido(ev)
            salida.append(ev)
        self.ahora = hasta
        return salida

    def eventos(self, paso_simulado: float = 10.0) -> Iterator[list[dict]]:
        while True:
            yield self.avanzar(paso_simulado)


def _poisson(rng: random.Random, lam: float) -> int:
    """Muestra Poisson (Knuth). Suficiente para lam pequeno (< 50)."""
    if lam <= 0:
        return 0
    if lam > 50:
        return max(0, round(rng.gauss(lam, lam ** 0.5)))
    import math
    limite, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limite:
            return k
        k += 1


def _franja(hora: int) -> str:
    for nombre, (a, b) in {"MADRUGADA": (0, 6), "DESAYUNO": (6, 11), "ALMUERZO": (11, 15),
                           "TARDE": (15, 18), "CENA": (18, 22), "NOCHE": (22, 24)}.items():
        if a <= hora < b:
            return nombre
    raise ValueError(hora)
