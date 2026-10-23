"""Pruebas del contrato del evento (Semana 1, se ejecutan en CI desde la Semana 2)."""
import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "samples" / "order_event.schema.json").read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA)


def eventos_jsonl():
    lineas = (ROOT / "samples" / "order_events.sample.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(l) for l in lineas if l.strip()]


def evento_base():
    return json.loads((ROOT / "samples" / "order_event.sample.json").read_text(encoding="utf-8"))


def test_schema_es_valido():
    Draft202012Validator.check_schema(SCHEMA)


@pytest.mark.parametrize("evento", [evento_base(), *eventos_jsonl()])
def test_samples_cumplen_contrato(evento):
    errores = [e.message for e in VALIDATOR.iter_errors(evento)]
    assert errores == []
    uuid.UUID(evento["event_id"])
    datetime.fromisoformat(evento["event_ts"].replace("Z", "+00:00"))


def test_zonas_del_contrato_coinciden_con_catalogo():
    catalogo = yaml.safe_load((ROOT / "config" / "zonas.yaml").read_text(encoding="utf-8"))
    ids = {z["zone_id"] for z in catalogo["zonas"]}
    assert set(SCHEMA["properties"]["zone_id"]["enum"]) == ids


@pytest.mark.parametrize(
    "cambio",
    [
        {"zone_id": "BOG-XXX"},               # zona fuera del catalogo
        {"status": "PENDING"},                # estado no definido
        {"active_couriers_in_zone": -1},      # conteo negativo
        {"team_id": "equipo-99"},             # evento de otro equipo
        {"campo_extra": 1},                   # campo no declarado
    ],
)
def test_eventos_invalidos_se_rechazan(cambio):
    evento = {**evento_base(), **cambio}
    assert list(VALIDATOR.iter_errors(evento)), f"deberia fallar con {cambio}"


@pytest.mark.parametrize("campo", SCHEMA["required"])
def test_campos_obligatorios(campo):
    evento = evento_base()
    del evento[campo]
    assert list(VALIDATOR.iter_errors(evento))
