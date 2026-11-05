"""DAG de control de RapidGo - Variante 5: Demanda por zonas (Issue #12).

Spark Structured Streaming escribe el Data Lake de forma continua. Este DAG corre
cada hora (o a mano) y hace el control batch:

    verificar_fuentes -> validar_raw -> agregar_demanda_franja -> validar_curated -> generar_reporte
                                    \\-> resumen_alertas ------------------------------/

- Reintentos: 2 por tarea, con 30 s de espera (default_args).
- XCom: cada tarea devuelve un diccionario de metricas que reciben las siguientes
  y que termina en el reporte.
- Las reglas viven en rapidgo_calidad.py (Python puro) y se prueban en CI.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent))  # rapidgo_calidad.py vive junto al DAG
import rapidgo_calidad as rc  # noqa: E402

DATALAKE = Path(os.getenv("RAPIDGO_DATALAKE", "/opt/airflow/datalake"))
CONFIG = Path(os.getenv("RAPIDGO_CONFIG", "/opt/airflow/config"))

default_args = {
    "owner": "equipo-02",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="rapidgo_control_demanda",
    description="Calidad del Data Lake, demanda por franja y reporte - RapidGo variante 5",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["rapidgo", "variante-5", "equipo-02"],
    doc_md=__doc__,
)
def rapidgo_control_demanda():

    @task
    def verificar_fuentes() -> dict:
        """Hay datos raw y el ultimo llego hace pocos minutos (Spark esta escribiendo)."""
        umbrales = rc.cargar_yaml(CONFIG / "umbrales.yaml")
        resultado = rc.verificar_fuentes(DATALAKE, umbrales)
        print(f"Fuentes OK: {resultado}")
        return resultado

    @task
    def validar_raw(fuentes: dict) -> dict:
        """Volumen minimo y tasa de invalidos de los eventos recientes."""
        umbrales = rc.cargar_yaml(CONFIG / "umbrales.yaml")
        raw = rc.leer_parquet(DATALAKE / "raw" / "pedidos", ["ingest_ts", "is_valid", "motivo_invalido"])
        resultado = rc.validar_raw(raw, umbrales)
        print(f"Calidad raw OK: {resultado}")
        return resultado

    @task
    def agregar_demanda_franja(calidad_raw: dict) -> dict:
        """KPI demanda por franja: escribe datalake/curated/kpi_demanda_franja."""
        processed = rc.leer_parquet(DATALAKE / "processed" / "pedidos", ["fecha", "franja", "zone_id", "status"])
        franja = rc.agregar_demanda_franja(processed)
        filas = rc.escribir_demanda_franja(franja, DATALAKE / "curated" / "kpi_demanda_franja")
        resultado = {"filas_escritas": filas, "eventos_leidos": int(len(processed)),
                     "top_zonas": rc.top_zonas(franja)}
        print(f"Demanda por franja: {resultado}")
        return resultado

    @task
    def resumen_alertas(calidad_raw: dict) -> dict:
        alertas = rc.leer_parquet(DATALAKE / "curated" / "alertas",
                                  ["tipo", "severidad", "zone_id", "window_end"])
        resultado = rc.resumen_alertas(alertas)
        print(f"Alertas: {resultado}")
        return resultado

    @task
    def validar_curated(demanda: dict) -> dict:
        umbrales = rc.cargar_yaml(CONFIG / "umbrales.yaml")
        zonas = {z["zone_id"] for z in rc.cargar_yaml(CONFIG / "zonas.yaml")["zonas"]}
        franja = rc.leer_parquet(DATALAKE / "curated" / "kpi_demanda_franja")
        resultado = rc.validar_curated(franja, zonas, umbrales["franjas"])
        print(f"Curated OK: {resultado}")
        return resultado

    @task
    def generar_reporte(fuentes: dict, calidad_raw: dict, demanda: dict, curated: dict, alertas: dict) -> str:
        texto = rc.generar_reporte(fuentes, calidad_raw, demanda, curated, alertas)
        carpeta = DATALAKE / "reports"
        carpeta.mkdir(parents=True, exist_ok=True)
        archivo = carpeta / f"reporte_{rc.ahora_utc():%Y%m%d_%H%M%S}.md"
        archivo.write_text(texto, encoding="utf-8")
        (carpeta / "ultimo_reporte.md").write_text(texto, encoding="utf-8")
        print(texto)
        return str(archivo)

    fuentes = verificar_fuentes()
    calidad = validar_raw(fuentes)
    demanda = agregar_demanda_franja(calidad)
    alertas = resumen_alertas(calidad)
    curated = validar_curated(demanda)
    generar_reporte(fuentes, calidad, demanda, curated, alertas)


rapidgo_control_demanda()
