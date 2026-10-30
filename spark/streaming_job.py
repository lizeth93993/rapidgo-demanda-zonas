"""Spark Structured Streaming - RapidGo Variante 5: Demanda por zonas.

Lee rapidgo.demanda.pedidos.v1, valida y transforma los eventos, calcula KPIs por
ventana de tiempo, evalua las alertas y escribe todo en el Data Lake (Parquet)
con checkpoint por query.

Queries (cada una con su checkpoint en datalake/_checkpoints/<nombre>):
  1. raw_pedidos          -> datalake/raw/pedidos                 (todo, incluso invalidos)
  2. processed_pedidos    -> datalake/processed/pedidos           (validos, deduplicados, con franja)
  3. kpi_demanda_minuto   -> datalake/curated/kpi_demanda_minuto  (ventana tumbling 1 min)
  4. zonas_calientes      -> datalake/curated/kpi_zonas_calientes + alertas DEMANDA_ALTA (5 min / 1 min)
  5. capacidad            -> datalake/curated/alertas CAPACIDAD_INSUFICIENTE (15 min / 5 min)

Transformaciones: (1) parseo JSON con el schema del contrato, (2) validacion de
negocio -> is_valid / motivo_invalido, (3) event_ts -> timestamp y hora local de
Bogota, (4) deduplicacion por event_id, (5) franja horaria.

Ejecucion normal (dentro del contenedor, ver docker-compose.yml):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 spark/streaming_job.py

Prueba local sin Kafka (lee archivos .jsonl de una carpeta):
  python spark/streaming_job.py --fuente archivos --entrada /ruta/eventos --duracion 90
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI))
import reglas  # noqa: E402

ROOT = AQUI.parent
ZONA_HORARIA = "America/Bogota"

ESQUEMA_EVENTO = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("schema_version", T.StringType()),
    T.StructField("team_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("zone_id", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("active_couriers_in_zone", T.IntegerType()),
    T.StructField("channel", T.StringType()),
])

ESQUEMA_ALERTA = T.StructType([
    T.StructField("alert_id", T.StringType()),
    T.StructField("tipo", T.StringType()),
    T.StructField("severidad", T.StringType()),
    T.StructField("zone_id", T.StringType()),
    T.StructField("window_start", T.TimestampType()),
    T.StructField("window_end", T.TimestampType()),
    T.StructField("valor", T.DoubleType()),
    T.StructField("umbral", T.DoubleType()),
    T.StructField("pedidos", T.LongType()),
    T.StructField("repartidores_min", T.LongType()),
    T.StructField("detalle", T.StringType()),
    T.StructField("generated_ts", T.TimestampType()),
    T.StructField("team_id", T.StringType()),
    T.StructField("fecha", T.DateType()),
])

ESQUEMA_CALIENTES = T.StructType([
    T.StructField("window_start", T.TimestampType()),
    T.StructField("window_end", T.TimestampType()),
    T.StructField("zone_id", T.StringType()),
    T.StructField("pedidos", T.LongType()),
    T.StructField("opm", T.DoubleType()),
    T.StructField("baseline_opm", T.DoubleType()),
    T.StructField("indice_calor", T.DoubleType()),
    T.StructField("es_caliente", T.BooleanType()),
    T.StructField("ranking_opm", T.IntegerType()),
    T.StructField("team_id", T.StringType()),
    T.StructField("fecha", T.DateType()),
])


# --------------------------------------------------------------------------- #
# Argumentos y sesion
# --------------------------------------------------------------------------- #
def parsear_argumentos(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--fuente", choices=["kafka", "archivos"], default=os.getenv("FUENTE", "kafka"))
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "rapidgo.demanda.pedidos.v1"))
    p.add_argument("--topic-alertas", default=os.getenv("KAFKA_TOPIC_ALERTAS", "rapidgo.demanda.alertas.v1"))
    p.add_argument("--entrada", help="carpeta con .jsonl (solo con --fuente archivos)")
    p.add_argument("--datalake", default=os.getenv("DATALAKE", str(ROOT / "datalake")))
    p.add_argument("--config", default=os.getenv("CONFIG_DIR", str(ROOT / "config")))
    p.add_argument("--trigger", default=os.getenv("TRIGGER", "10 seconds"))
    p.add_argument("--duracion", type=float, default=0, help="segundos a ejecutar; 0 = sin limite")
    return p.parse_args(argv)


def crear_sesion() -> SparkSession:
    spark = (SparkSession.builder.appName("rapidgo-demanda-zonas-equipo-02")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- #
# Lectura
# --------------------------------------------------------------------------- #
def leer_fuente(spark: SparkSession, args) -> DataFrame:
    if args.fuente == "kafka":
        df = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", 20000)
              .load())
        return df.select(
            F.col("value").cast("string").alias("json_raw"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
    if not args.entrada:
        raise SystemExit("--entrada es obligatorio con --fuente archivos")
    df = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(args.entrada)
    return df.select(
        F.col("value").alias("json_raw"),
        F.lit(None).cast("string").alias("kafka_key"),
        F.lit(-1).alias("kafka_partition"),
        F.lit(None).cast("long").alias("kafka_offset"),
        F.current_timestamp().alias("kafka_ts"),
    )


# --------------------------------------------------------------------------- #
# Transformaciones
# --------------------------------------------------------------------------- #
def expresion_franja(columna_hora, franjas: dict):
    expr = None
    for nombre, (inicio, fin) in franjas.items():
        cond = (columna_hora >= inicio) & (columna_hora < fin)
        expr = F.when(cond, nombre) if expr is None else expr.when(cond, nombre)
    return expr.otherwise("DESCONOCIDA")


def transformar(df: DataFrame) -> DataFrame:
    """T1 parseo + T2 validacion + T3 timestamps. Devuelve todas las filas (validas e invalidas)."""
    e = F.from_json("json_raw", ESQUEMA_EVENTO)
    df = df.withColumn("e", e).select("*", "e.*").drop("e")
    df = df.withColumn("event_time", F.to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    motivo = (
        F.when(F.col("event_id").isNull(), "json invalido o sin event_id")
        .when(F.col("team_id").isNull() | (F.col("team_id") != reglas.TEAM_ID), "team_id distinto")
        .when(F.col("zone_id").isNull(), "falta zone_id")
        .when(~F.col("zone_id").isin(*sorted(reglas.ZONAS_VALIDAS)), "zona desconocida")
        .when(~F.col("status").isin(*sorted(reglas.ESTADOS_VALIDOS)), "estado desconocido")
        .when(F.col("active_couriers_in_zone").isNull() | (F.col("active_couriers_in_zone") < 0),
              "repartidores invalido")
        .when(F.col("event_time").isNull(), "event_ts invalido")
    )
    return (df.withColumn("motivo_invalido", motivo)
              .withColumn("is_valid", F.col("motivo_invalido").isNull())
              .withColumn("ingest_ts", F.current_timestamp())
              .withColumn("hora_local", F.from_utc_timestamp("event_time", ZONA_HORARIA)))


def validos(df: DataFrame, watermark: str) -> DataFrame:
    return df.filter("is_valid").withWatermark("event_time", watermark)


# --------------------------------------------------------------------------- #
# Escritura de lotes de alertas (foreachBatch)
# --------------------------------------------------------------------------- #
class Escritor:
    def __init__(self, spark: SparkSession, args, umbrales: dict, zonas: dict):
        self.spark = spark
        self.args = args
        self.umbrales = umbrales
        self.zonas = zonas
        self.dl = Path(args.datalake)

    def _fecha(self, ts: datetime):
        return (ts.replace(tzinfo=timezone.utc).astimezone(_tz_bogota())).date()

    def _alerta(self, tipo, severidad, fila, valor, umbral, detalle) -> tuple:
        clave = f"{tipo}|{fila['zone_id']}|{fila['window_start'].isoformat()}"
        return (
            str(uuid.uuid5(uuid.NAMESPACE_URL, clave)), tipo, severidad, fila["zone_id"],
            fila["window_start"], fila["window_end"], float(valor), float(umbral),
            int(fila["pedidos"]), int(fila["repartidores_min"]) if fila.get("repartidores_min") is not None else None,
            detalle, datetime.now(timezone.utc).replace(tzinfo=None), reglas.TEAM_ID, self._fecha(fila["window_end"]),
        )

    def _guardar_alertas(self, filas: list[tuple], batch_id: int) -> None:
        if not filas:
            return
        df = self.spark.createDataFrame(filas, ESQUEMA_ALERTA)
        df.write.mode("append").partitionBy("fecha").parquet(str(self.dl / "curated" / "alertas"))
        for f in filas:
            print(f"[ALERTA] batch={batch_id} {f[1]} {f[2]} zona={f[3]} ventana={f[4]:%H:%M}-{f[5]:%H:%M} "
                  f"valor={f[6]:.2f} umbral={f[7]:.2f} pedidos={f[8]} repartidores_min={f[9]}", flush=True)
        if self.args.fuente == "kafka":
            try:
                (df.select(F.col("zone_id").alias("key"),
                           F.to_json(F.struct(*[c for c in df.columns])).alias("value"))
                   .write.format("kafka")
                   .option("kafka.bootstrap.servers", self.args.bootstrap)
                   .option("topic", self.args.topic_alertas).save())
            except Exception as exc:  # el Parquet ya quedo escrito; no detener el pipeline
                print(f"[WARN] no se pudo publicar alertas en Kafka: {exc}", flush=True)

    def zonas_calientes(self, batch_df: DataFrame, batch_id: int) -> None:
        minutos_v = reglas.minutos(self.umbrales["alertas"]["DEMANDA_ALTA"]["ventana"])
        filas = [r.asDict() for r in batch_df.collect()]
        if not filas:
            return
        kpis, alertas = [], []
        # ranking de opm por ventana para el "top N" de zonas calientes
        por_ventana: dict = {}
        for f in filas:
            f["opm"] = reglas.pedidos_por_minuto(f["pedidos"], minutos_v)
            f["baseline_opm"] = float(self.zonas[f["zone_id"]]["baseline_opm"])
            f["indice_calor"] = reglas.indice_calor(f["opm"], f["baseline_opm"])
            por_ventana.setdefault(f["window_start"], []).append(f)
        for grupo in por_ventana.values():
            for pos, f in enumerate(sorted(grupo, key=lambda x: -x["opm"]), start=1):
                f["ranking_opm"] = pos
        regla = self.umbrales["alertas"]["DEMANDA_ALTA"]
        for f in filas:
            caliente = reglas.es_zona_caliente(f["indice_calor"], self.umbrales)
            kpis.append((f["window_start"], f["window_end"], f["zone_id"], int(f["pedidos"]), f["opm"],
                         f["baseline_opm"], f["indice_calor"], caliente, f["ranking_opm"], reglas.TEAM_ID,
                         self._fecha(f["window_end"])))
            sev = reglas.severidad_demanda(f["opm"], f["indice_calor"], self.umbrales)
            if sev:
                umbral = regla["critical" if sev == reglas.CRITICAL else "warning"]["indice_calor"]
                f["repartidores_min"] = None
                alertas.append(self._alerta("DEMANDA_ALTA", sev, f, f["indice_calor"], umbral,
                                            f"{f['opm']:.1f} pedidos/min vs {f['baseline_opm']:.1f} normales"))
        (self.spark.createDataFrame(kpis, ESQUEMA_CALIENTES).write.mode("append").partitionBy("fecha")
             .parquet(str(self.dl / "curated" / "kpi_zonas_calientes")))
        self._guardar_alertas(alertas, batch_id)

    def capacidad(self, batch_df: DataFrame, batch_id: int) -> None:
        regla = self.umbrales["alertas"]["CAPACIDAD_INSUFICIENTE"]
        alertas = []
        for r in batch_df.collect():
            f = r.asDict()
            sev = reglas.severidad_capacidad(int(f["pedidos"]), int(f["repartidores_min"]), self.umbrales)
            if sev:
                p = reglas.presion(int(f["pedidos"]), int(f["repartidores_min"]))
                umbral = regla["critical" if sev == reglas.CRITICAL else "warning"]["presion"]
                valor = p if p != float("inf") else 9999.0
                alertas.append(self._alerta("CAPACIDAD_INSUFICIENTE", sev, f, valor, umbral,
                                            f"{f['pedidos']} pedidos para {f['repartidores_min']} repartidores"))
        self._guardar_alertas(alertas, batch_id)


def _tz_bogota():
    from datetime import timedelta
    return timezone(timedelta(hours=-5))   # Bogota no usa horario de verano


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #
def iniciar_queries(spark: SparkSession, args) -> list:
    umbrales = reglas.cargar_umbrales(Path(args.config) / "umbrales.yaml")
    zonas = reglas.cargar_zonas(Path(args.config) / "zonas.yaml")
    dl = Path(args.datalake)
    chk = dl / "_checkpoints"
    wm = umbrales["kpis"]["pedidos_por_minuto"]["watermark"]
    trig = {"processingTime": args.trigger}
    escritor = Escritor(spark, args, umbrales, zonas)

    base = transformar(leer_fuente(spark, args))
    queries = []

    # 1. RAW: todo lo que llega, con metadatos de Kafka y la marca de validez
    raw = base.select("json_raw", "kafka_key", "kafka_partition", "kafka_offset", "kafka_ts", "ingest_ts",
                      "event_id", "zone_id", "status", "is_valid", "motivo_invalido",
                      F.coalesce(F.to_date("hora_local"), F.to_date("ingest_ts")).alias("fecha"))
    queries.append(raw.writeStream.queryName("raw_pedidos").format("parquet")
                   .option("path", str(dl / "raw" / "pedidos"))
                   .option("checkpointLocation", str(chk / "raw_pedidos"))
                   .partitionBy("fecha").trigger(**trig).outputMode("append").start())

    # 2. PROCESSED: validos, deduplicados por event_id, con franja horaria
    franjas = umbrales["franjas"]
    processed = (validos(base, wm)
                 .dropDuplicatesWithinWatermark(["event_id"])
                 .withColumn("franja", expresion_franja(F.hour("hora_local"), franjas))
                 .withColumn("fecha", F.to_date("hora_local"))
                 .select("event_id", "order_id", "zone_id", "category", "status", "channel",
                         "active_couriers_in_zone", "event_time", "hora_local", "franja",
                         "kafka_partition", "kafka_offset", "ingest_ts", "team_id", "fecha"))
    queries.append(processed.writeStream.queryName("processed_pedidos").format("parquet")
                   .option("path", str(dl / "processed" / "pedidos"))
                   .option("checkpointLocation", str(chk / "processed_pedidos"))
                   .partitionBy("fecha", "zone_id").trigger(**trig).outputMode("append").start())

    creados = validos(base, wm).filter(F.col("status") == "CREATED")

    # 3. KPI pedidos por minuto (ventana tumbling)
    v1 = umbrales["kpis"]["pedidos_por_minuto"]["ventana"]
    por_minuto = (creados.groupBy(F.window("event_time", v1).alias("w"), "zone_id")
                  .agg(F.count("*").alias("pedidos"))
                  .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                          "zone_id", "pedidos",
                          (F.col("pedidos") / reglas.minutos(v1)).alias("opm"),
                          F.lit(reglas.TEAM_ID).alias("team_id"),
                          F.to_date(F.from_utc_timestamp(F.col("w.end"), ZONA_HORARIA)).alias("fecha")))
    queries.append(por_minuto.writeStream.queryName("kpi_demanda_minuto").format("parquet")
                   .option("path", str(dl / "curated" / "kpi_demanda_minuto"))
                   .option("checkpointLocation", str(chk / "kpi_demanda_minuto"))
                   .partitionBy("fecha").trigger(**trig).outputMode("append").start())

    # 4. Zonas calientes + DEMANDA_ALTA (ventana deslizante)
    rd = umbrales["alertas"]["DEMANDA_ALTA"]
    calientes = (creados.groupBy(F.window("event_time", rd["ventana"], rd["deslizamiento"]).alias("w"), "zone_id")
                 .agg(F.count("*").alias("pedidos"))
                 .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                         "zone_id", "pedidos"))
    queries.append(calientes.writeStream.queryName("zonas_calientes")
                   .option("checkpointLocation", str(chk / "zonas_calientes"))
                   .foreachBatch(escritor.zonas_calientes).trigger(**trig).outputMode("append").start())

    # 5. CAPACIDAD_INSUFICIENTE (ventana deslizante larga, todos los estados para ver repartidores)
    rc = umbrales["alertas"]["CAPACIDAD_INSUFICIENTE"]
    capacidad = (validos(base, wm)
                 .groupBy(F.window("event_time", rc["ventana"], rc["deslizamiento"]).alias("w"), "zone_id")
                 .agg(F.sum(F.when(F.col("status") == "CREATED", 1).otherwise(0)).cast("long").alias("pedidos"),
                      F.min("active_couriers_in_zone").cast("long").alias("repartidores_min"))
                 .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                         "zone_id", "pedidos", "repartidores_min"))
    queries.append(capacidad.writeStream.queryName("capacidad")
                   .option("checkpointLocation", str(chk / "capacidad"))
                   .foreachBatch(escritor.capacidad).trigger(**trig).outputMode("append").start())
    return queries


def reportar(queries: list) -> None:
    for q in queries:
        p = q.lastProgress
        if p:
            print(f"[progreso] {q.name:<20} batch={p['batchId']:<4} filas_entrada={p['numInputRows']:<6} "
                  f"filas/s={p.get('processedRowsPerSecond', 0):.1f} "
                  f"watermark={p.get('eventTime', {}).get('watermark', '-')}", flush=True)


def main(argv=None) -> int:
    args = parsear_argumentos(argv)
    # Las fechas que devuelve collect() usan la zona horaria del proceso Python: se fija en UTC.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    spark = crear_sesion()
    queries = iniciar_queries(spark, args)
    print(f"Pipeline iniciado: {len(queries)} queries | fuente={args.fuente} | datalake={args.datalake}", flush=True)
    t0 = time.monotonic()
    try:
        while True:
            if spark.streams.awaitAnyTermination(30):
                break
            reportar(queries)
            if args.duracion and time.monotonic() - t0 >= args.duracion:
                break
    finally:
        for q in queries:
            if q.exception():
                print(f"[ERROR] query {q.name}: {q.exception()}", flush=True)
            q.stop()
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
