"""Verificacion de punta a punta del pipeline de RapidGo (variante 5) - Issue #15.

Revisa, con todo levantado (`docker compose up -d --build`), que cada pieza funcione:

  Kafka     topics creados, 3 particiones y mensajes llegando (offsets que crecen)
  Data Lake raw, processed y curated con datos; raw fresco; alertas de los dos tipos
  Airflow   API sana y ultima ejecucion del DAG en success

Uso (desde la carpeta del repo, con el .venv activado):
  python scripts/verificar_pipeline.py
  python scripts/verificar_pipeline.py --markdown        # tabla lista para pegar en README / PR
  python scripts/verificar_pipeline.py --sin-airflow     # si Airflow esta apagado

Codigo de salida 0 si no hay ninguna [FALLA]; 1 si la hay. Solo usa la libreria
estandar, confluent-kafka y las funciones de airflow/dags/rapidgo_calidad.py.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))
import rapidgo_calidad as rc  # noqa: E402

OK, AVISO, FALLA = "OK", "AVISO", "FALLA"
TOPIC_PEDIDOS = "rapidgo.demanda.pedidos.v1"
TOPIC_ALERTAS = "rapidgo.demanda.alertas.v1"
DAG_ID = "rapidgo_control_demanda"

TABLAS = [  # (ruta dentro del datalake, nombre, obligatoria)
    ("raw/pedidos", "raw/pedidos", True),
    ("processed/pedidos", "processed/pedidos", True),
    ("curated/kpi_demanda_minuto", "KPI pedidos por minuto", True),
    ("curated/kpi_zonas_calientes", "KPI zonas calientes", True),
    ("curated/alertas", "alertas", True),
    ("curated/kpi_demanda_franja", "KPI demanda por franja (Airflow)", False),
]


@dataclass
class Resultado:
    componente: str
    control: str
    estado: str
    detalle: str


# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #
def offsets_topic(bootstrap: str, topic: str, timeout: float = 10.0) -> dict[int, int]:
    """Offset final (high watermark) de cada particion."""
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "rapidgo-verificador", "enable.auto.commit": False})
    try:
        meta = c.list_topics(topic, timeout=timeout).topics.get(topic)
        if meta is None or meta.error is not None:
            return {}
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout)[1] for p in sorted(meta.partitions)}
    finally:
        c.close()


def evaluar_kafka(antes: dict[int, int], despues: dict[int, int], particiones_esperadas: int = 3) -> list[Resultado]:
    if not antes:
        return [Resultado("Kafka", f"topic {TOPIC_PEDIDOS}", FALLA, "no existe o Kafka no responde")]
    res = []
    n = len(antes)
    res.append(Resultado("Kafka", "particiones del topic", OK if n == particiones_esperadas else FALLA,
                         f"{n} particiones (esperadas {particiones_esperadas}, ADR-001)"))
    total = sum(despues.values())
    reparto = ", ".join(f"p{p}={despues[p] / total:.0%}" for p in sorted(despues)) if total else "-"
    nuevos = total - sum(antes.values())
    res.append(Resultado("Kafka", "mensajes en el topic", OK if total > 0 else FALLA,
                         f"{total:,} mensajes ({reparto})"))
    res.append(Resultado("Kafka", "producer enviando", OK if nuevos > 0 else FALLA,
                         f"{nuevos:,} mensajes nuevos durante la medicion" if nuevos > 0
                         else "no llegaron mensajes nuevos: revisar `docker compose ps producer`"))
    return res


# --------------------------------------------------------------------------- #
# Data Lake
# --------------------------------------------------------------------------- #
def evaluar_datalake(datalake: Path, umbrales: dict, ahora=None) -> list[Resultado]:
    datalake = Path(datalake)
    res = []
    for ruta, nombre, obligatoria in TABLAS:
        archivos = rc.archivos_parquet(datalake / ruta)
        if not archivos:
            estado = FALLA if obligatoria else AVISO
            nota = "sin datos" + ("" if obligatoria else ": correr el DAG en Airflow")
            res.append(Resultado("Data Lake", nombre, estado, nota))
            continue
        filas = len(rc.leer_parquet(datalake / ruta, ["zone_id"]))
        res.append(Resultado("Data Lake", nombre, OK, f"{filas:,} filas en {len(archivos)} archivos"))
    try:
        fuentes = rc.verificar_fuentes(datalake, umbrales, ahora=ahora)
        res.append(Resultado("Data Lake", "raw fresco", OK,
                             f"ultimo evento hace {fuentes['minutos_desde_ultimo']} min"))
    except rc.FuenteNoDisponible as exc:
        res.append(Resultado("Data Lake", "raw fresco", FALLA, str(exc)))
    alertas = rc.leer_parquet(datalake / "curated" / "alertas", ["tipo", "severidad"])
    if len(alertas):
        tipos = set(alertas["tipo"])
        faltan = {"DEMANDA_ALTA", "CAPACIDAD_INSUFICIENTE"} - tipos
        conteo = ", ".join(f"{t}={n}" for t, n in alertas["tipo"].value_counts().sort_index().items())
        res.append(Resultado("Data Lake", "alertas de los dos tipos", AVISO if faltan else OK,
                             conteo + (f"; falta {', '.join(sorted(faltan))}: provocar un pico (PICO_ZONA/REDUCIR_ZONA)"
                                       if faltan else "")))
    reporte = datalake / "reports" / "ultimo_reporte.md"
    res.append(Resultado("Data Lake", "reporte de Airflow", OK if reporte.exists() else AVISO,
                         str(reporte.relative_to(datalake.parent)) if reporte.exists() else "aun no hay reporte"))
    return res


# --------------------------------------------------------------------------- #
# Airflow
# --------------------------------------------------------------------------- #
def _get_json(url: str, token: Optional[str] = None, timeout: float = 10.0):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def consultar_airflow(base: str) -> tuple[Optional[dict], Optional[dict]]:
    salud = _get_json(f"{base}/api/v2/monitor/health")
    token = _get_json(f"{base}/auth/token").get("access_token")
    runs = _get_json(f"{base}/api/v2/dags/{DAG_ID}/dagRuns?order_by=-start_date&limit=1", token)
    return salud, runs


def evaluar_airflow(salud: Optional[dict], runs: Optional[dict]) -> list[Resultado]:
    if salud is None:
        return [Resultado("Airflow", "API", FALLA, "no responde en el puerto 8081: `docker compose logs airflow`")]
    componentes = {k: v.get("status") for k, v in salud.items() if isinstance(v, dict) and "status" in v}
    malos = [k for k, v in componentes.items() if v != "healthy"]
    res = [Resultado("Airflow", "salud", FALLA if malos else OK,
                     ", ".join(f"{k}={v}" for k, v in sorted(componentes.items())))]
    ultimos = (runs or {}).get("dag_runs") or []
    if not ultimos:
        res.append(Resultado("Airflow", f"ultima ejecucion de {DAG_ID}", AVISO, "nunca se ha ejecutado: activar el DAG"))
    else:
        r = ultimos[0]
        estado = {"success": OK, "failed": FALLA}.get(r.get("state"), AVISO)
        res.append(Resultado("Airflow", f"ultima ejecucion de {DAG_ID}", estado,
                             f"{r.get('state')} ({r.get('run_type')}, inicio {str(r.get('start_date'))[:19]} UTC)"))
    return res


# --------------------------------------------------------------------------- #
# Salida
# --------------------------------------------------------------------------- #
def imprimir(resultados: list[Resultado], markdown: bool) -> None:
    if markdown:
        print("| Componente | Control | Estado | Detalle |\n|---|---|---|---|")
        for r in resultados:
            print(f"| {r.componente} | {r.control} | {r.estado} | {r.detalle} |")
        return
    for r in resultados:
        print(f"[{r.estado:<5}] {r.componente:<9} {r.control:<34} {r.detalle}")


def codigo_salida(resultados: list[Resultado]) -> int:
    return 1 if any(r.estado == FALLA for r in resultados) else 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Verificacion de punta a punta del pipeline RapidGo")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--datalake", default=str(ROOT / "datalake"))
    p.add_argument("--airflow", default="http://localhost:8081")
    p.add_argument("--espera", type=float, default=5.0, help="segundos entre las dos lecturas de offsets")
    p.add_argument("--sin-kafka", action="store_true")
    p.add_argument("--sin-airflow", action="store_true")
    p.add_argument("--markdown", action="store_true")
    args = p.parse_args(argv)

    umbrales = rc.cargar_yaml(ROOT / "config" / "umbrales.yaml")
    resultados: list[Resultado] = []
    if not args.sin_kafka:
        try:
            antes = offsets_topic(args.bootstrap, TOPIC_PEDIDOS)
            time.sleep(args.espera if antes else 0)
            despues = offsets_topic(args.bootstrap, TOPIC_PEDIDOS) if antes else {}
            resultados += evaluar_kafka(antes, despues)
            alertas_kafka = offsets_topic(args.bootstrap, TOPIC_ALERTAS)
            total = sum(alertas_kafka.values())
            resultados.append(Resultado("Kafka", f"topic {TOPIC_ALERTAS}", OK if alertas_kafka else FALLA,
                                        f"{total:,} alertas publicadas" if alertas_kafka else "no existe"))
        except Exception as exc:  # Kafka apagado, confluent-kafka sin instalar, etc.
            resultados.append(Resultado("Kafka", "conexion", FALLA, f"{type(exc).__name__}: {exc}"))
    resultados += evaluar_datalake(Path(args.datalake), umbrales)
    if not args.sin_airflow:
        try:
            resultados += evaluar_airflow(*consultar_airflow(args.airflow))
        except Exception as exc:
            resultados += evaluar_airflow(None, None)
            resultados[-1].detalle += f" ({type(exc).__name__})"
    imprimir(resultados, args.markdown)
    codigo = codigo_salida(resultados)
    if not args.markdown:
        print("\nResultado:", "TODO OK" if codigo == 0 else "HAY FALLAS (ver lineas [FALLA])")
    return codigo


if __name__ == "__main__":
    sys.exit(main())
