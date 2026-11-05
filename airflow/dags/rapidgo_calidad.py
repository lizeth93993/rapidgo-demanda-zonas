"""Reglas de calidad y agregacion batch de RapidGo (variante 5) - Python puro, sin Airflow.

Lo usa el DAG airflow/dags/rapidgo_control_demanda.py y lo prueba tests/test_calidad.py
en CI. Lee el Data Lake que escribe Spark (Parquet con particiones fecha=...).

Cada validacion devuelve un diccionario con metricas (viaja por XCom al reporte) o
lanza una excepcion con un mensaje claro; en Airflow esa excepcion marca la tarea
como fallida y activa los reintentos.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import yaml

FRANJAS_ORDEN = ["MADRUGADA", "DESAYUNO", "ALMUERZO", "TARDE", "CENA", "NOCHE"]


class FuenteNoDisponible(Exception):
    """El Data Lake no tiene datos recientes: Kafka, el producer o Spark estan detenidos."""


class CalidadError(Exception):
    """Los datos llegan, pero no cumplen las reglas de calidad."""


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def cargar_yaml(ruta: Path) -> dict:
    return yaml.safe_load(Path(ruta).read_text(encoding="utf-8"))


def ahora_utc() -> datetime:
    """Hora actual en UTC sin zona (igual que los timestamps que escribe Spark)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def archivos_parquet(ruta: Path) -> list[Path]:
    """Archivos de datos de una tabla, ignorando _spark_metadata y ocultos."""
    ruta = Path(ruta)
    if not ruta.exists():
        return []
    return [p for p in ruta.rglob("*.parquet")
            if not any(parte.startswith(("_", ".")) for parte in p.relative_to(ruta).parts)]


def leer_parquet(ruta: Path, columnas: Optional[list[str]] = None) -> pd.DataFrame:
    """Lee una tabla del Data Lake (con particiones hive). DataFrame vacio si no hay datos."""
    archivos = archivos_parquet(ruta)
    if not archivos:
        return pd.DataFrame(columns=columnas or [])
    dataset = ds.dataset([str(a) for a in archivos], format="parquet",
                         partitioning=ds.partitioning(flavor="hive"), partition_base_dir=str(ruta))
    nombres = set(dataset.schema.names)
    cols = [c for c in columnas if c in nombres] if columnas else None
    return dataset.to_table(columns=cols).to_pandas()


def _a_texto(valor) -> Optional[str]:
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    if isinstance(valor, (pd.Timestamp, datetime)):
        return pd.Timestamp(valor).strftime("%Y-%m-%d %H:%M:%S")
    return str(valor)


# --------------------------------------------------------------------------- #
# Tarea 1: verificar fuentes
# --------------------------------------------------------------------------- #
def verificar_fuentes(datalake: Path, umbrales: dict, ahora: Optional[datetime] = None) -> dict:
    """Comprueba que Spark esta escribiendo: hay datos raw y el ultimo es reciente."""
    ahora = ahora or ahora_utc()
    regla = umbrales["calidad"]
    raw_dir = Path(datalake) / "raw" / "pedidos"
    archivos = archivos_parquet(raw_dir)
    if not archivos:
        raise FuenteNoDisponible(f"No hay archivos Parquet en {raw_dir}: Spark no ha escrito datos raw.")
    if not (Path(datalake) / "_checkpoints").exists():
        raise FuenteNoDisponible("No existe datalake/_checkpoints: el job de Spark nunca ha corrido.")
    raw = leer_parquet(raw_dir, ["ingest_ts"])
    ultimo = pd.to_datetime(raw["ingest_ts"]).max()
    minutos = (pd.Timestamp(ahora) - ultimo).total_seconds() / 60
    if minutos > regla["max_minutos_sin_datos"]:
        raise FuenteNoDisponible(
            f"El ultimo evento raw se ingirio hace {minutos:.1f} min (limite {regla['max_minutos_sin_datos']} min). "
            "Revisar que el producer, Kafka y Spark esten corriendo (docker compose ps).")
    return {"archivos_raw": len(archivos), "eventos_raw_total": int(len(raw)),
            "ultimo_ingest_utc": _a_texto(ultimo), "minutos_desde_ultimo": round(minutos, 2)}


# --------------------------------------------------------------------------- #
# Tarea 2: validar raw (ventana reciente)
# --------------------------------------------------------------------------- #
def validar_raw(raw: pd.DataFrame, umbrales: dict, ahora: Optional[datetime] = None) -> dict:
    """Valida los eventos ingeridos en los ultimos N minutos: volumen minimo y tasa de invalidos."""
    ahora = ahora or ahora_utc()
    regla = umbrales["calidad"]
    desde = pd.Timestamp(ahora) - timedelta(minutes=regla["ventana_validacion_min"])
    reciente = raw[pd.to_datetime(raw["ingest_ts"]) >= desde] if len(raw) else raw
    total = int(len(reciente))
    if total < regla["min_eventos_raw"]:
        raise CalidadError(
            f"Solo {total} eventos raw en los ultimos {regla['ventana_validacion_min']} min "
            f"(minimo {regla['min_eventos_raw']}): la fuente esta caida o casi sin trafico.")
    invalidos = reciente[~reciente["is_valid"].astype(bool)]
    tasa = len(invalidos) / total
    motivos = {str(k): int(v) for k, v in invalidos["motivo_invalido"].value_counts().items()}
    metricas = {"ventana_min": regla["ventana_validacion_min"], "eventos": total,
                "invalidos": int(len(invalidos)), "tasa_invalidos": round(tasa, 4), "motivos": motivos}
    if tasa > regla["max_tasa_invalidos"]:
        raise CalidadError(
            f"Tasa de eventos invalidos {tasa:.1%} en los ultimos {regla['ventana_validacion_min']} min "
            f"supera el limite de {regla['max_tasa_invalidos']:.0%}. Motivos: {motivos}")
    return metricas


# --------------------------------------------------------------------------- #
# Tarea 3: agregar demanda por franja (KPI 3)
# --------------------------------------------------------------------------- #
def agregar_demanda_franja(processed: pd.DataFrame) -> pd.DataFrame:
    """Pedidos creados y cancelados por fecha, franja y zona."""
    columnas = ["fecha", "franja", "zone_id", "pedidos", "cancelados", "tasa_cancelacion"]
    if processed.empty:
        return pd.DataFrame(columns=columnas)
    df = processed.copy()
    df["fecha"] = df["fecha"].astype(str)
    df["creado"] = (df["status"] == "CREATED").astype(int)
    df["cancelado"] = (df["status"] == "CANCELLED").astype(int)
    agg = (df.groupby(["fecha", "franja", "zone_id"], as_index=False)
             .agg(pedidos=("creado", "sum"), cancelados=("cancelado", "sum")))
    agg["tasa_cancelacion"] = (agg["cancelados"] / agg["pedidos"].where(agg["pedidos"] > 0)).fillna(0).round(4)
    agg["franja"] = pd.Categorical(agg["franja"], categories=FRANJAS_ORDEN + ["DESCONOCIDA"], ordered=True)
    agg = agg.sort_values(["fecha", "franja", "pedidos"], ascending=[True, True, False])
    agg["franja"] = agg["franja"].astype(str)
    agg["pedidos"] = agg["pedidos"].astype("int64")
    agg["cancelados"] = agg["cancelados"].astype("int64")
    return agg[columnas].reset_index(drop=True)


def escribir_demanda_franja(df: pd.DataFrame, destino: Path) -> int:
    """Reescribe curated/kpi_demanda_franja (idempotente: correr dos veces da lo mismo)."""
    destino = Path(destino)
    tabla = pa.Table.from_pandas(df.assign(team_id="equipo-02"), preserve_index=False)
    pq.write_to_dataset(tabla, root_path=str(destino), partition_cols=["fecha"],
                        existing_data_behavior="delete_matching")
    return len(df)


# --------------------------------------------------------------------------- #
# Tarea 4: resumen de alertas
# --------------------------------------------------------------------------- #
def resumen_alertas(alertas: pd.DataFrame) -> dict:
    if alertas.empty:
        return {"total": 0, "por_tipo": {}, "criticas_por_zona": {}, "ultima": None}
    por_tipo = {f"{t} {s}": int(n) for (t, s), n in alertas.groupby(["tipo", "severidad"]).size().items()}
    criticas = alertas[alertas["severidad"] == "CRITICAL"]
    por_zona = {str(z): int(n) for z, n in criticas["zone_id"].value_counts().items()}
    ultima = alertas.sort_values("window_end").iloc[-1]
    return {"total": int(len(alertas)), "por_tipo": por_tipo, "criticas_por_zona": por_zona,
            "ultima": {"tipo": ultima["tipo"], "severidad": ultima["severidad"], "zona": ultima["zone_id"],
                       "ventana_fin_utc": _a_texto(ultima["window_end"])}}


# --------------------------------------------------------------------------- #
# Tarea 5: validar curated
# --------------------------------------------------------------------------- #
def validar_curated(franja: pd.DataFrame, zonas_validas: set[str], franjas: dict) -> dict:
    if franja.empty:
        raise CalidadError("kpi_demanda_franja quedo vacio: no hay eventos validos en processed.")
    desconocidas = sorted(set(franja["zone_id"]) - set(zonas_validas))
    if desconocidas:
        raise CalidadError(f"Zonas desconocidas en kpi_demanda_franja: {desconocidas}")
    franjas_malas = sorted(set(franja["franja"]) - set(franjas))
    if franjas_malas:
        raise CalidadError(f"Franjas no definidas en umbrales.yaml: {franjas_malas}")
    if (franja["pedidos"] < 0).any() or (franja["cancelados"] < 0).any():
        raise CalidadError("Hay conteos negativos en kpi_demanda_franja.")
    return {"filas": int(len(franja)), "zonas": int(franja["zone_id"].nunique()),
            "franjas": sorted(set(franja["franja"])), "pedidos": int(franja["pedidos"].sum())}


# --------------------------------------------------------------------------- #
# Tarea 6: reporte
# --------------------------------------------------------------------------- #
def top_zonas(franja: pd.DataFrame, n: int = 3) -> list[dict]:
    tot = franja.groupby("zone_id")["pedidos"].sum().sort_values(ascending=False).head(n)
    return [{"zona": str(z), "pedidos": int(p)} for z, p in tot.items()]


def generar_reporte(fuentes: dict, calidad_raw: dict, demanda: dict, curated: dict, alertas: dict,
                    generado_utc: Optional[str] = None) -> str:
    generado_utc = generado_utc or ahora_utc().strftime("%Y-%m-%d %H:%M:%S")
    lineas = [
        "# Reporte de demanda por zonas - RapidGo (equipo-02)",
        "",
        f"Generado: {generado_utc} UTC por el DAG `rapidgo_control_demanda`.",
        "",
        "## Calidad de datos",
        "",
        "| Control | Resultado |",
        "|---|---|",
        f"| Ultimo evento raw | hace {fuentes['minutos_desde_ultimo']} min ({fuentes['ultimo_ingest_utc']} UTC) |",
        f"| Eventos raw (total en el Data Lake) | {fuentes['eventos_raw_total']} |",
        f"| Eventos en los ultimos {calidad_raw['ventana_min']} min | {calidad_raw['eventos']} |",
        f"| Tasa de invalidos | {calidad_raw['tasa_invalidos']:.2%} ({calidad_raw['invalidos']} eventos) |",
        f"| Filas en kpi_demanda_franja | {curated['filas']} ({curated['zonas']} zonas) |",
        "",
        "## Demanda",
        "",
        f"Pedidos creados en el Data Lake: **{curated['pedidos']}**. Franjas con datos: {', '.join(curated['franjas'])}.",
        "",
        "| Zona | Pedidos |",
        "|---|---|",
    ]
    lineas += [f"| {z['zona']} | {z['pedidos']} |" for z in demanda.get("top_zonas", [])]
    lineas += ["", "## Alertas", "", f"Total de alertas registradas: **{alertas['total']}**.", ""]
    if alertas["por_tipo"]:
        lineas += ["| Tipo y severidad | Cantidad |", "|---|---|"]
        lineas += [f"| {k} | {v} |" for k, v in sorted(alertas["por_tipo"].items())]
    if alertas["criticas_por_zona"]:
        zonas = ", ".join(f"{z} ({n})" for z, n in alertas["criticas_por_zona"].items())
        lineas += ["", f"Zonas con alertas criticas: {zonas}."]
    if alertas.get("ultima"):
        u = alertas["ultima"]
        lineas += ["", f"Ultima alerta: {u['tipo']} {u['severidad']} en {u['zona']} (ventana hasta {u['ventana_fin_utc']} UTC)."]
    return "\n".join(lineas) + "\n"
