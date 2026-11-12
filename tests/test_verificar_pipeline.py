"""Pruebas del verificador de punta a punta (scripts/verificar_pipeline.py). No necesitan Docker."""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import verificar_pipeline as vp  # noqa: E402

U = yaml.safe_load((ROOT / "config" / "umbrales.yaml").read_text(encoding="utf-8"))
AHORA = datetime(2026, 11, 12, 15, 0)


def escribir(dl: Path, ruta: str, df: pd.DataFrame, particion: str = "fecha=2026-11-12"):
    destino = dl / ruta / particion
    destino.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), destino / f"part-{len(list(destino.iterdir()))}.parquet")


def datalake_completo(tmp_path: Path, minutos_atras=1, tipos=("DEMANDA_ALTA", "CAPACIDAD_INSUFICIENTE")) -> Path:
    ts = AHORA - timedelta(minutes=minutos_atras)
    escribir(tmp_path, "raw/pedidos", pd.DataFrame({"zone_id": ["BOG-CHA"] * 5, "ingest_ts": [ts] * 5}))
    for ruta in ("processed/pedidos", "curated/kpi_demanda_minuto", "curated/kpi_zonas_calientes",
                 "curated/kpi_demanda_franja"):
        escribir(tmp_path, ruta, pd.DataFrame({"zone_id": ["BOG-CHA", "BOG-SUB"]}))
    for tipo in tipos:   # misma estructura que escribe Spark tras el fix del Issue #15
        escribir(tmp_path, f"curated/alertas/tipo={tipo}", pd.DataFrame({"zone_id": ["BOG-CHA"], "severidad": ["WARNING"]}))
    (tmp_path / "_checkpoints").mkdir()
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports" / "ultimo_reporte.md").write_text("# Reporte\n")
    return tmp_path


def estados(resultados):
    return {r.control: r.estado for r in resultados}


# --------------------------------- Kafka ----------------------------------- #
def test_kafka_ok():
    e = estados(vp.evaluar_kafka({0: 100, 1: 250, 2: 150}, {0: 110, 1: 270, 2: 160}))
    assert e == {"particiones del topic": "OK", "mensajes en el topic": "OK", "producer enviando": "OK"}


def test_kafka_producer_detenido():
    assert estados(vp.evaluar_kafka({0: 10, 1: 20, 2: 30}, {0: 10, 1: 20, 2: 30}))["producer enviando"] == "FALLA"


def test_kafka_particiones_incorrectas():
    assert estados(vp.evaluar_kafka({0: 1}, {0: 2}))["particiones del topic"] == "FALLA"


def test_kafka_sin_topic():
    r = vp.evaluar_kafka({}, {})
    assert len(r) == 1 and r[0].estado == "FALLA"


# ------------------------------- Data Lake --------------------------------- #
def test_datalake_completo(tmp_path):
    r = vp.evaluar_datalake(datalake_completo(tmp_path), U, ahora=AHORA)
    assert all(x.estado == "OK" for x in r), [(x.control, x.estado, x.detalle) for x in r]
    assert vp.codigo_salida(r) == 0


def test_alertas_leidas_por_tipo(tmp_path):
    r = estados(vp.evaluar_datalake(datalake_completo(tmp_path, tipos=("DEMANDA_ALTA",)), U, ahora=AHORA))
    assert r["alertas de los dos tipos"] == "AVISO"


def test_raw_viejo_es_falla(tmp_path):
    r = vp.evaluar_datalake(datalake_completo(tmp_path, minutos_atras=30), U, ahora=AHORA)
    assert estados(r)["raw fresco"] == "FALLA"
    assert vp.codigo_salida(r) == 1


def test_datalake_vacio(tmp_path):
    e = estados(vp.evaluar_datalake(tmp_path, U, ahora=AHORA))
    assert e["raw/pedidos"] == "FALLA"
    assert e["KPI demanda por franja (Airflow)"] == "AVISO"     # opcional: lo escribe el DAG


# -------------------------------- Airflow ---------------------------------- #
SALUD_OK = {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"},
            "dag_processor": {"status": "healthy"}, "triggerer": {"status": "healthy"}}


def test_airflow_ok():
    runs = {"dag_runs": [{"state": "success", "run_type": "manual", "start_date": "2026-11-12T15:00:00Z"}]}
    assert all(r.estado == "OK" for r in vp.evaluar_airflow(SALUD_OK, runs))


def test_airflow_scheduler_caido():
    salud = {**SALUD_OK, "scheduler": {"status": "unhealthy"}}
    assert estados(vp.evaluar_airflow(salud, {"dag_runs": []}))["salud"] == "FALLA"


def test_airflow_ultimo_run_fallido():
    runs = {"dag_runs": [{"state": "failed", "run_type": "scheduled", "start_date": "2026-11-12T14:00:00Z"}]}
    assert vp.codigo_salida(vp.evaluar_airflow(SALUD_OK, runs)) == 1


def test_airflow_no_responde():
    r = vp.evaluar_airflow(None, None)
    assert r[0].estado == "FALLA"


def test_salida_markdown(capsys):
    vp.imprimir([vp.Resultado("Kafka", "x", "OK", "y")], markdown=True)
    assert "| Kafka | x | OK | y |" in capsys.readouterr().out
