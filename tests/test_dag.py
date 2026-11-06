"""Valida que el DAG de Airflow se importa sin errores y tiene la estructura esperada.

Necesita apache-airflow instalado: en CI corre en el job "validar-dag". En un computador
sin Airflow estas pruebas se saltan (skipped).
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models.dagbag")  # la carpeta airflow/ del repo no cuenta como Airflow

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_ID = "rapidgo_control_demanda"


@pytest.fixture(scope="module")
def dagbag():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=str(DAGS), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[DAG_ID]


def test_sin_errores_de_importacion(dagbag):
    assert dagbag.import_errors == {}


def test_dag_registrado(dagbag):
    assert DAG_ID in dagbag.dags


def test_al_menos_5_tareas(dag):
    assert len(dag.tasks) >= 5


def test_reintentos_en_todas_las_tareas(dag):
    for t in dag.tasks:
        assert t.retries >= 1, t.task_id


def test_dependencias(dag):
    up = {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}
    assert up["verificar_fuentes"] == set()
    assert up["validar_raw"] == {"verificar_fuentes"}
    assert up["agregar_demanda_franja"] == {"validar_raw"}
    assert up["resumen_alertas"] == {"validar_raw"}
    assert up["validar_curated"] == {"agregar_demanda_franja"}
    assert {"validar_curated", "resumen_alertas"} <= up["generar_reporte"]


def test_sin_catchup_y_con_horario(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
