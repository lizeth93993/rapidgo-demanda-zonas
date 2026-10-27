"""Pruebas de las reglas de negocio (spark/reglas.py). Corren en CI sin Spark."""
import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))
import reglas  # noqa: E402

U = reglas.cargar_umbrales()
ZONAS = reglas.cargar_zonas()


# ------------------------------- franja ------------------------------------ #
@pytest.mark.parametrize("hora,esperada", [
    (0, "MADRUGADA"), (5, "MADRUGADA"), (6, "DESAYUNO"), (10, "DESAYUNO"),
    (11, "ALMUERZO"), (14, "ALMUERZO"), (15, "TARDE"), (18, "CENA"), (21, "CENA"),
    (22, "NOCHE"), (23, "NOCHE"),
])
def test_franja(hora, esperada):
    assert reglas.franja(hora, U["franjas"]) == esperada


def test_franjas_cubren_las_24_horas():
    assert [reglas.franja(h, U["franjas"]) for h in range(24)]


def test_franja_hora_invalida():
    with pytest.raises(ValueError):
        reglas.franja(24, U["franjas"])


# ------------------------------- KPIs -------------------------------------- #
def test_pedidos_por_minuto():
    assert reglas.pedidos_por_minuto(45, 5) == 10.0


def test_indice_calor_demanda_normal_es_1():
    assert reglas.indice_calor(9.0, ZONAS["BOG-CHA"]["baseline_opm"]) == pytest.approx(1.0)


def test_zona_caliente():
    assert reglas.es_zona_caliente(1.5, U)
    assert not reglas.es_zona_caliente(1.49, U)


def test_presion():
    assert reglas.presion(150, 100) == 1.5
    assert reglas.presion(5, 0) == math.inf
    assert reglas.presion(0, 0) == 0.0


# ------------------------------ DEMANDA_ALTA ------------------------------- #
@pytest.mark.parametrize("opm,indice,esperada", [
    (9.0, 1.0, None),          # demanda normal en Chapinero
    (13.5, 1.5, "WARNING"),    # justo en el umbral warning
    (18.0, 2.0, "CRITICAL"),   # justo en el umbral critical
    (22.5, 2.5, "CRITICAL"),
    (2.8, 1.9, None),          # zona pequena: indice alto pero menos de 3 opm -> sin alerta
    (3.0, 1.6, "WARNING"),     # en el minimo absoluto ya alerta
])
def test_severidad_demanda(opm, indice, esperada):
    assert reglas.severidad_demanda(opm, indice, U) == esperada


# -------------------------- CAPACIDAD_INSUFICIENTE ------------------------- #
@pytest.mark.parametrize("pedidos,repartidores,esperada", [
    (135, 190, None),          # Chapinero normal: 9 opm * 15 min / 190 = 0.71
    (190, 190, "WARNING"),     # presion 1.0
    (285, 190, "CRITICAL"),    # presion 1.5
    (0, 0, None),              # sin pedidos no hay alerta
    (3, 0, "CRITICAL"),        # pedidos y ningun repartidor
])
def test_severidad_capacidad(pedidos, repartidores, esperada):
    assert reglas.severidad_capacidad(pedidos, repartidores, U) == esperada


def test_demanda_normal_no_genera_alertas_en_ninguna_zona():
    """Con la demanda base de cada zona y sus repartidores base, ninguna regla debe disparar."""
    for z in ZONAS.values():
        opm = z["baseline_opm"]
        assert reglas.severidad_demanda(opm, 1.0, U) is None
        pedidos_15 = round(opm * 15)
        assert reglas.severidad_capacidad(pedidos_15, z["repartidores_base"], U) is None, z["zone_id"]


def test_pico_de_1_5x_dispara_ambas_reglas_en_warning():
    """Coherencia de umbrales documentada en el README: un pico de 1.5x activa las dos reglas."""
    z = ZONAS["BOG-CHA"]
    opm = z["baseline_opm"] * 1.5
    assert reglas.severidad_demanda(opm, 1.5, U) == "WARNING"
    assert reglas.severidad_capacidad(round(opm * 15), z["repartidores_base"], U) == "WARNING"


# ----------------------------- validacion ---------------------------------- #
def evento_ok():
    return {"event_id": "e1", "order_id": "ORD-20261022-ABCDEF", "zone_id": "BOG-CHA", "status": "CREATED",
            "event_ts": "2026-10-22T17:31:05.412Z", "active_couriers_in_zone": 10, "team_id": "equipo-02"}


def test_evento_valido():
    assert reglas.validar_evento(evento_ok()) == (True, "ok")


@pytest.mark.parametrize("cambio,motivo", [
    ({"zone_id": "BOG-XXX"}, "zona desconocida"),
    ({"status": "PENDING"}, "estado desconocido"),
    ({"active_couriers_in_zone": -1}, "repartidores invalido"),
    ({"team_id": "equipo-99"}, "team_id distinto"),
    ({"zone_id": None}, "falta zone_id"),
])
def test_evento_invalido(cambio, motivo):
    assert reglas.validar_evento({**evento_ok(), **cambio}) == (False, motivo)


def test_ventanas_de_umbrales_en_minutos():
    assert reglas.minutos(U["alertas"]["DEMANDA_ALTA"]["ventana"]) == 5
    assert reglas.minutos(U["alertas"]["CAPACIDAD_INSUFICIENTE"]["ventana"]) == 15
