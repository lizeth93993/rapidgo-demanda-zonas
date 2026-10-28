"""Pruebas del generador de eventos (producer/generador.py). No necesitan Kafka."""
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "producer"))
from generador import Escenario, Generador, cargar_zonas  # noqa: E402

VALIDADOR = Draft202012Validator(json.loads((ROOT / "samples" / "order_event.schema.json").read_text(encoding="utf-8")))
INICIO = datetime(2026, 10, 29, 17, 0, tzinfo=timezone.utc)   # 12:00 en Bogota


def generar(minutos=30, **kw):
    esc = Escenario(tasa_invalidos=kw.pop("tasa_invalidos", 0.0), **kw)
    g = Generador(cargar_zonas(), esc, inicio=INICIO, seed=42)
    eventos = []
    for _ in range(minutos * 6):
        eventos += g.avanzar(10)
    return eventos


def test_todos_los_eventos_cumplen_el_contrato():
    eventos = generar(20)
    assert len(eventos) > 500
    errores = [e for e in eventos if list(VALIDADOR.iter_errors(e))]
    assert errores == []


def test_eventos_ordenados_por_tiempo():
    eventos = generar(20)
    tiempos = [e["event_ts"] for e in eventos]
    assert tiempos == sorted(tiempos)


def test_ciclo_de_vida_en_orden():
    orden = {"CREATED": 0, "ASSIGNED": 1, "PICKED_UP": 2, "DELIVERED": 3, "CANCELLED": 1}
    por_pedido = {}
    for e in generar(60):
        por_pedido.setdefault(e["order_id"], []).append(e["status"])
    completos = [s for s in por_pedido.values() if s[0] == "CREATED"]
    assert completos
    for estados in completos:
        assert [orden[s] for s in estados] == sorted(orden[s] for s in estados)
        assert estados.count("CREATED") == 1


def test_demanda_normal_cerca_del_baseline():
    """En 60 minutos, los CREATED por zona deben estar cerca de baseline_opm * 60."""
    creados = Counter(e["zone_id"] for e in generar(60) if e["status"] == "CREATED")
    for z in cargar_zonas():
        esperado = z["baseline_opm"] * 60
        assert abs(creados[z["zone_id"]] - esperado) < 0.35 * esperado + 10, z["zone_id"]


def test_pico_multiplica_la_demanda_de_la_zona():
    normal = Counter(e["zone_id"] for e in generar(20) if e["status"] == "CREATED")
    pico = Counter(e["zone_id"] for e in generar(20, pico_zona="BOG-CHA", pico_factor=3.0,
                                                pico_inicio_min=0, pico_duracion_min=60)
                   if e["status"] == "CREATED")
    assert pico["BOG-CHA"] > 2 * normal["BOG-CHA"]


def test_reducir_repartidores():
    eventos = generar(10, reducir_zona="BOG-SUB", reducir_factor=0.5, pico_inicio_min=0, pico_duracion_min=60)
    sub = [e["active_couriers_in_zone"] for e in eventos if e["zone_id"] == "BOG-SUB"]
    assert max(sub) <= 160 * 1.05 * 0.5 + 1


def test_eventos_invalidos_a_proposito_no_cumplen_el_contrato():
    eventos = generar(20, tasa_invalidos=0.2)
    invalidos = [e for e in eventos if list(VALIDADOR.iter_errors(e))]
    assert 0.1 < len(invalidos) / len(eventos) < 0.3


def test_identificable_por_equipo():
    assert {e["team_id"] for e in generar(5)} == {"equipo-02"}


def test_zona_desconocida_en_escenario():
    with pytest.raises(ValueError):
        Generador(cargar_zonas(), Escenario(pico_zona="BOG-XXX"))


def test_cli_dry_run():
    salida = subprocess.run([sys.executable, str(ROOT / "producer" / "producer.py"), "--dry-run",
                             "--max-eventos", "15", "--tasa-invalidos", "0", "--seed", "1"],
                            capture_output=True, text=True, timeout=60, check=True).stdout.strip().splitlines()
    assert len(salida) == 15
    for linea in salida:
        assert not list(VALIDADOR.iter_errors(json.loads(linea)))
