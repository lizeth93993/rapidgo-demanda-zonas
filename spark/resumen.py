"""Resumen del Data Lake: evidencia real para el checkpoint de Semana 2 y el ADR-002.

Dentro del contenedor (con el pipeline corriendo o detenido):
  docker compose exec spark /opt/spark/bin/spark-submit /app/spark/resumen.py

Imprime: filas por capa, eventos por particion de Kafka (balance de la key zone_id),
eventos invalidos por motivo, alertas por tipo/severidad/zona y top de zonas calientes.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]


def leer(spark, ruta: Path):
    try:
        return spark.read.parquet(str(ruta))
    except Exception:  # la capa aun no tiene datos
        return None


def titulo(texto: str) -> None:
    print(f"\n=== {texto} ===", flush=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--datalake", default=os.getenv("DATALAKE", str(ROOT / "datalake")))
    args = p.parse_args()
    dl = Path(args.datalake)

    spark = (SparkSession.builder.appName("rapidgo-resumen")
             .config("spark.sql.session.timeZone", "America/Bogota")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw = leer(spark, dl / "raw" / "pedidos")
    proc = leer(spark, dl / "processed" / "pedidos")
    kpi = leer(spark, dl / "curated" / "kpi_demanda_minuto")
    cal = leer(spark, dl / "curated" / "kpi_zonas_calientes")
    ale = leer(spark, dl / "curated" / "alertas")

    titulo("Filas por capa")
    for nombre, df in [("raw/pedidos", raw), ("processed/pedidos", proc), ("curated/kpi_demanda_minuto", kpi),
                       ("curated/kpi_zonas_calientes", cal), ("curated/alertas", ale)]:
        print(f"{nombre:<30} {df.count() if df is not None else 0:>10}")

    if raw is not None:
        titulo("Eventos por particion de Kafka (balance de la key zone_id, ADR-001)")
        total = raw.count()
        (raw.groupBy("kafka_partition").agg(F.count("*").alias("eventos"),
                                            F.countDistinct("zone_id").alias("zonas"),
                                            F.concat_ws(",", F.sort_array(F.collect_set("zone_id"))).alias("lista"))
            .withColumn("porcentaje", F.round(F.col("eventos") * 100 / total, 1))
            .orderBy("kafka_partition").show(truncate=False))
        titulo("Eventos invalidos por motivo")
        raw.filter("not is_valid").groupBy("motivo_invalido").count().orderBy(F.desc("count")).show(truncate=False)

    if proc is not None:
        titulo("Eventos validos por estado")
        proc.groupBy("status").count().orderBy(F.desc("count")).show()

    if kpi is not None:
        titulo("Pedidos por minuto por zona (promedio y maximo observado)")
        (kpi.groupBy("zone_id").agg(F.round(F.avg("opm"), 2).alias("opm_promedio"), F.max("opm").alias("opm_max"),
                                    F.count("*").alias("minutos"))
            .orderBy(F.desc("opm_promedio")).show())

    if cal is not None:
        titulo("Zonas calientes (ventanas con indice de calor >= umbral)")
        (cal.filter("es_caliente").groupBy("zone_id")
            .agg(F.count("*").alias("ventanas_calientes"), F.round(F.max("indice_calor"), 2).alias("indice_max"))
            .orderBy(F.desc("ventanas_calientes")).show())

    if ale is not None:
        titulo("Alertas por tipo, severidad y zona")
        ale.groupBy("tipo", "severidad", "zone_id").count().orderBy("tipo", F.desc("count")).show(50, truncate=False)
        titulo("Ultimas 10 alertas")
        (ale.orderBy(F.desc("window_end"))
            .select("tipo", "severidad", "zone_id", "window_start", "window_end", F.round("valor", 2).alias("valor"),
                    "umbral", "pedidos", "repartidores_min").show(10, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
