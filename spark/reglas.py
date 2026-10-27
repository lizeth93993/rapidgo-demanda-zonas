"""Reglas de negocio de la variante 5 (Demanda por zonas) en Python puro.

Este modulo NO importa pyspark: lo usan
  - spark/streaming_job.py, para evaluar las alertas en cada micro-batch, y
  - tests/test_reglas.py, para probar las reglas en CI sin levantar Spark.
Asi la logica que se prueba es exactamente la que corre en el pipeline.

Los umbrales vienen de config/umbrales.yaml (justificacion en el README).
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]

ZONAS_VALIDAS = {
    "BOG-CHA", "BOG-USA", "BOG-SUB", "BOG-TEU", "BOG-ENG", "BOG-KEN",
    "BOG-FON", "BOG-BUN", "BOG-PAR", "BOG-BOS", "BOG-CAN", "BOG-CBO",
}
ESTADOS_VALIDOS = {"CREATED", "ASSIGNED", "PICKED_UP", "DELIVERED", "CANCELLED"}
CATEGORIAS_VALIDAS = {"RESTAURANTE", "MERCADO", "FARMACIA", "LICORES", "MASCOTAS", "OTROS"}
TEAM_ID = "equipo-02"

WARNING = "WARNING"
CRITICAL = "CRITICAL"


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
def cargar_umbrales(ruta: Optional[Path] = None) -> dict:
    ruta = Path(ruta) if ruta else ROOT / "config" / "umbrales.yaml"
    return yaml.safe_load(ruta.read_text(encoding="utf-8"))


def cargar_zonas(ruta: Optional[Path] = None) -> dict[str, dict]:
    """Devuelve {zone_id: {...datos de la zona...}}."""
    ruta = Path(ruta) if ruta else ROOT / "config" / "zonas.yaml"
    datos = yaml.safe_load(ruta.read_text(encoding="utf-8"))
    return {z["zone_id"]: z for z in datos["zonas"]}


# --------------------------------------------------------------------------- #
# Transformaciones
# --------------------------------------------------------------------------- #
def franja(hora_local: int, franjas: dict[str, list[int]]) -> str:
    """Nombre de la franja horaria para una hora local 0-23. Rangos [inicio, fin)."""
    if not 0 <= hora_local <= 23:
        raise ValueError(f"hora fuera de rango: {hora_local}")
    for nombre, (inicio, fin) in franjas.items():
        if inicio <= hora_local < fin:
            return nombre
    raise ValueError(f"la hora {hora_local} no cae en ninguna franja")


def validar_evento(evento: dict) -> tuple[bool, str]:
    """Validacion de negocio minima (la misma que aplica Spark para marcar is_valid)."""
    obligatorios = ("event_id", "order_id", "zone_id", "status", "event_ts", "active_couriers_in_zone")
    for campo in obligatorios:
        if evento.get(campo) in (None, ""):
            return False, f"falta {campo}"
    if evento.get("team_id") != TEAM_ID:
        return False, "team_id distinto"
    if evento["zone_id"] not in ZONAS_VALIDAS:
        return False, "zona desconocida"
    if evento["status"] not in ESTADOS_VALIDOS:
        return False, "estado desconocido"
    couriers = evento["active_couriers_in_zone"]
    if not isinstance(couriers, int) or isinstance(couriers, bool) or couriers < 0:
        return False, "repartidores invalido"
    return True, "ok"


# --------------------------------------------------------------------------- #
# KPIs
# --------------------------------------------------------------------------- #
def pedidos_por_minuto(pedidos: int, minutos_ventana: float) -> float:
    if minutos_ventana <= 0:
        raise ValueError("la ventana debe ser positiva")
    return pedidos / minutos_ventana


def indice_calor(opm: float, baseline_opm: float) -> float:
    """opm observado / opm normal de la zona. 1.0 = demanda normal."""
    if baseline_opm <= 0:
        raise ValueError("baseline_opm debe ser positivo")
    return opm / baseline_opm


def es_zona_caliente(indice: float, umbrales: dict) -> bool:
    return indice >= umbrales["kpis"]["zona_caliente"]["indice_calor_min"]


def presion(pedidos: int, repartidores: int) -> float:
    """Pedidos creados en la ventana por cada repartidor activo. inf si no hay repartidores."""
    if repartidores < 0:
        raise ValueError("repartidores no puede ser negativo")
    if repartidores == 0:
        return math.inf if pedidos > 0 else 0.0
    return pedidos / repartidores


# --------------------------------------------------------------------------- #
# Alertas
# --------------------------------------------------------------------------- #
def severidad_demanda(opm: float, indice: float, umbrales: dict) -> Optional[str]:
    """DEMANDA_ALTA: None, WARNING o CRITICAL."""
    regla = umbrales["alertas"]["DEMANDA_ALTA"]
    if opm < regla["minimo_absoluto_opm"]:
        return None
    if indice >= regla["critical"]["indice_calor"]:
        return CRITICAL
    if indice >= regla["warning"]["indice_calor"]:
        return WARNING
    return None


def severidad_capacidad(pedidos: int, repartidores_min: int, umbrales: dict) -> Optional[str]:
    """CAPACIDAD_INSUFICIENTE: None, WARNING o CRITICAL."""
    regla = umbrales["alertas"]["CAPACIDAD_INSUFICIENTE"]
    if pedidos <= 0:
        return None
    if repartidores_min == 0:
        return CRITICAL if regla.get("sin_repartidores", CRITICAL) == "critical" else WARNING
    p = presion(pedidos, repartidores_min)
    if p >= regla["critical"]["presion"]:
        return CRITICAL
    if p >= regla["warning"]["presion"]:
        return WARNING
    return None


def minutos(texto: str) -> int:
    """'5 minutes' -> 5. Para leer las ventanas de umbrales.yaml."""
    numero, unidad = texto.split()
    if not unidad.startswith("minute"):
        raise ValueError(f"unidad no soportada: {texto}")
    return int(numero)
