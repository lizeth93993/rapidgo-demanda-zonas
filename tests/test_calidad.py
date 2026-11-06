"""Pruebas de las reglas de calidad del DAG (airflow/dags/rapidgo_calidad.py). No necesitan Airflow."""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))
import rapidgo_calidad as rc  # noqa: E402

U = yaml.safe_load((ROOT / "config" / "umbrales.yaml").read_text(encoding="utf-8"))
ZONAS = {z["zone_id"] for z in yaml.safe_load((ROOT / "config" / "zonas.yaml").read_text(encoding="utf-8"))["zonas"]}
AHORA = datetime(2026, 11, 5, 17, 0, 0)


def raw_df(n=500, invalidos=5, minutos_atras=1):
    ts = AHORA - timedelta(minutes=minutos_atras)
    filas = [{"ingest_ts": ts, "is_valid": True, "motivo_invalido": None} for _ in range(n - invalidos)]
    filas += [{"ingest_ts": ts, "is_valid": False, "motivo_invalido": "zona desconocida"} for _ in range(invalidos)]
    return pd.DataFrame(filas)


def escribir_raw(dl: Path, df: pd.DataFrame):
    destino = dl / "raw" / "pedidos" / "fecha=2026-11-05"
    destino.mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(df), destino / "part-0000.snappy.parquet")
    (dl / "raw" / "pedidos" / "_spark_metadata").mkdir()
    (dl / "raw" / "pedidos" / "_spark_metadata" / "0").write_text("v1")
    (dl / "_checkpoints").mkdir()


# ------------------------------ leer_parquet ------------------------------- #
def test_leer_parquet_con_particiones_e_ignora_metadata(tmp_path):
    escribir_raw(tmp_path, raw_df(10, 0))
    df = rc.leer_parquet(tmp_path / "raw" / "pedidos")
    assert len(df) == 10
    assert "fecha" in df.columns          # columna de la particion hive


def test_leer_parquet_carpeta_inexistente(tmp_path):
    assert rc.leer_parquet(tmp_path / "no-existe", ["a"]).empty


# ---------------------------- verificar_fuentes ---------------------------- #
def test_fuentes_ok(tmp_path):
    escribir_raw(tmp_path, raw_df(minutos_atras=2))
    r = rc.verificar_fuentes(tmp_path, U, ahora=AHORA)
    assert r["eventos_raw_total"] == 500 and r["minutos_desde_ultimo"] == 2.0


def test_fuente_detenida(tmp_path):
    escribir_raw(tmp_path, raw_df(minutos_atras=20))
    with pytest.raises(rc.FuenteNoDisponible, match="hace 20.0 min"):
        rc.verificar_fuentes(tmp_path, U, ahora=AHORA)


def test_sin_datos_raw(tmp_path):
    with pytest.raises(rc.FuenteNoDisponible, match="No hay archivos"):
        rc.verificar_fuentes(tmp_path, U, ahora=AHORA)


# ------------------------------- validar_raw ------------------------------- #
def test_raw_ok():
    r = rc.validar_raw(raw_df(500, 5), U, ahora=AHORA)
    assert r["tasa_invalidos"] == 0.01 and r["motivos"] == {"zona desconocida": 5}


def test_raw_demasiados_invalidos():
    with pytest.raises(rc.CalidadError, match="Tasa de eventos invalidos 30.0%"):
        rc.validar_raw(raw_df(500, 150), U, ahora=AHORA)


def test_raw_poco_volumen():
    with pytest.raises(rc.CalidadError, match="Solo 50 eventos"):
        rc.validar_raw(raw_df(50, 0), U, ahora=AHORA)


def test_raw_solo_cuenta_la_ventana_reciente():
    """Eventos invalidos viejos (fuera de la ventana) no hacen fallar la validacion: asi se recupera."""
    viejos = raw_df(500, 400, minutos_atras=60)
    recientes = raw_df(500, 5, minutos_atras=1)
    r = rc.validar_raw(pd.concat([viejos, recientes]), U, ahora=AHORA)
    assert r["eventos"] == 500


# -------------------------- agregar_demanda_franja ------------------------- #
def processed_df():
    filas = []
    for zona, franja, creados, cancelados in [("BOG-CHA", "ALMUERZO", 10, 1), ("BOG-CHA", "CENA", 6, 0),
                                              ("BOG-SUB", "ALMUERZO", 4, 2)]:
        filas += [{"fecha": "2026-11-05", "franja": franja, "zone_id": zona, "status": "CREATED"}] * creados
        filas += [{"fecha": "2026-11-05", "franja": franja, "zone_id": zona, "status": "CANCELLED"}] * cancelados
        filas += [{"fecha": "2026-11-05", "franja": franja, "zone_id": zona, "status": "DELIVERED"}] * 3
    return pd.DataFrame(filas)


def test_agregar_demanda_franja():
    df = rc.agregar_demanda_franja(processed_df())
    fila = df[(df.zone_id == "BOG-CHA") & (df.franja == "ALMUERZO")].iloc[0]
    assert (fila.pedidos, fila.cancelados, fila.tasa_cancelacion) == (10, 1, 0.1)
    assert list(df.franja) == ["ALMUERZO", "ALMUERZO", "CENA"]      # orden del dia


def test_escritura_idempotente(tmp_path):
    df = rc.agregar_demanda_franja(processed_df())
    destino = tmp_path / "kpi"
    rc.escribir_demanda_franja(df, destino)
    rc.escribir_demanda_franja(df, destino)                          # segunda corrida del DAG
    assert len(rc.leer_parquet(destino)) == len(df)


# ------------------------------ validar_curated ---------------------------- #
def test_curated_ok():
    r = rc.validar_curated(rc.agregar_demanda_franja(processed_df()), ZONAS, U["franjas"])
    assert r == {"filas": 3, "zonas": 2, "franjas": ["ALMUERZO", "CENA"], "pedidos": 20}


def test_curated_vacio():
    with pytest.raises(rc.CalidadError, match="vacio"):
        rc.validar_curated(rc.agregar_demanda_franja(pd.DataFrame()), ZONAS, U["franjas"])


def test_curated_zona_desconocida():
    df = rc.agregar_demanda_franja(processed_df().assign(zone_id="BOG-XXX"))
    with pytest.raises(rc.CalidadError, match="BOG-XXX"):
        rc.validar_curated(df, ZONAS, U["franjas"])


# ------------------------------ alertas y reporte -------------------------- #
def test_resumen_alertas():
    alertas = pd.DataFrame([
        {"tipo": "DEMANDA_ALTA", "severidad": "CRITICAL", "zone_id": "BOG-CHA", "window_end": AHORA},
        {"tipo": "DEMANDA_ALTA", "severidad": "WARNING", "zone_id": "BOG-PAR", "window_end": AHORA - timedelta(minutes=5)},
        {"tipo": "CAPACIDAD_INSUFICIENTE", "severidad": "CRITICAL", "zone_id": "BOG-SUB", "window_end": AHORA},
    ])
    r = rc.resumen_alertas(alertas)
    assert r["total"] == 3
    assert r["por_tipo"] == {"CAPACIDAD_INSUFICIENTE CRITICAL": 1, "DEMANDA_ALTA CRITICAL": 1, "DEMANDA_ALTA WARNING": 1}
    assert r["criticas_por_zona"] == {"BOG-CHA": 1, "BOG-SUB": 1}


def test_resumen_sin_alertas():
    assert rc.resumen_alertas(pd.DataFrame())["total"] == 0


def test_reporte_contiene_las_secciones():
    franja = rc.agregar_demanda_franja(processed_df())
    texto = rc.generar_reporte(
        {"minutos_desde_ultimo": 1.5, "ultimo_ingest_utc": "2026-11-05 16:58:30", "eventos_raw_total": 900},
        rc.validar_raw(raw_df(), U, ahora=AHORA),
        {"top_zonas": rc.top_zonas(franja)},
        rc.validar_curated(franja, ZONAS, U["franjas"]),
        {"total": 0, "por_tipo": {}, "criticas_por_zona": {}, "ultima": None},
        generado_utc="2026-11-05 17:00:00")
    for seccion in ("## Calidad de datos", "## Demanda", "## Alertas", "| BOG-CHA | 16 |"):
        assert seccion in texto


def test_umbrales_de_calidad_definidos():
    assert set(U["calidad"]) == {"max_minutos_sin_datos", "ventana_validacion_min", "min_eventos_raw",
                                 "max_tasa_invalidos"}
