#!/usr/bin/env python
"""
Driver batch: ingesta → limpieza → análisis → export.
"""

import glob
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
# ------------------ SparkSession ------------------ #
spark = (
    SparkSession.builder
    .appName("Ecobici Batch")
    .config("spark.sql.session.timeZone", "America/Mexico_City")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
from cleaning import clean_pipeline


BASE = Path("/app")
DATA_DIR = BASE / "data"
RESULTS = BASE / "results"
RESULTS.mkdir(exist_ok=True)

# ------------------ A) Ingesta ------------------ #
files = glob.glob(str(DATA_DIR / "*.csv"))
if not files:
    raise FileNotFoundError("No se encontraron CSV en /data")

df_raw = spark.read.csv(files, header=True, schema=None)  # strings

# ------------------ B) Limpieza ------------------ #
df_clean = clean_pipeline(df_raw)

# Nos quedamos con las 8 columnas y casteamos por seguridad
df_final = (
    df_clean.select(
        "estacion_origen", "estacion_destino",
        "fecha_hora_retiro", "fecha_hora_arribo",
        "genero", "edad_rango", "id_bici", "duracion_segundos"
    )
    .withColumn("estacion_origen", F.col("estacion_origen").cast("int"))
    .withColumn("estacion_destino", F.col("estacion_destino").cast("int"))
    .withColumn("id_bici", F.col("id_bici").cast("int"))
    .withColumn("duracion_segundos",F.col("duracion_segundos").cast("int"))
    .cache()
)

# ------------------ C) Métricas ------------------ #
# 1. Top rutas
top_rutas = (
    df_final.groupBy("estacion_origen", "estacion_destino")
            .count().orderBy(F.desc("count")).limit(20)
)

# 2. Heatmap hora/día
horas = (
    df_final.withColumn("hora", F.hour("fecha_hora_retiro"))
            .withColumn("dia_semana", F.dayofweek("fecha_hora_retiro"))
            .groupBy("dia_semana", "hora").count()
)

# 3. Balance estaciones
entr = df_final.groupBy("estacion_destino").count().withColumnRenamed("count","llegadas")
sal = df_final.groupBy("estacion_origen").count().withColumnRenamed("count","salidas")
balance = (entr.join(sal, entr.estacion_destino==sal.estacion_origen,"outer")
                 .fillna(0)
                 .withColumn("balance", F.col("llegadas")-F.col("salidas"))
                 .orderBy(F.desc("balance")).limit(20))

# 4. Estadísticas adicionales
dur_media = df_final.select(F.avg(F.col("duracion_segundos") / 60)).first()[0]
top_ori = sal.orderBy(F.desc("salidas")).first()["estacion_origen"]
top_des = entr.orderBy(F.desc("llegadas")).first()["estacion_destino"]

# ------------------ D) Export ------------------ #
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema_summary = StructType([
    StructField("metrica", StringType(), True),
    StructField("valor",   DoubleType(), True)
])

summary = spark.createDataFrame([
    ("total_viajes", float(df_final.count())),
    ("duracion_promedio_min", float(dur_media)),
    ("estacion_origen_top", float(top_ori)),
    ("estacion_destino_top", float(top_des)),
    ("balance_max", float(balance.first()["balance"]))
], schema=schema_summary)

out_summary = str(RESULTS / "resumen_ecobici")
out_top = str(RESULTS / "top_rutas")

summary.coalesce(1).write.mode("overwrite").csv(out_summary, header=True)
top_rutas.coalesce(1).write.mode("overwrite").csv(out_top, header=True)

# ------- Gráficos (matplotlib) ------- #
import matplotlib.pyplot as plt

# Heatmap
pivot = horas.toPandas().pivot(index="dia_semana", columns="hora", values="count").fillna(0)
plt.figure(figsize=(12,6))
plt.imshow(pivot, aspect="auto", origin="lower")
plt.title("Viajes por día y hora")
plt.xlabel("Hora")
plt.ylabel("Día (1=Lun)")
plt.colorbar(label="Viajes")
plt.tight_layout()
plt.savefig(RESULTS/"heatmap_horarios.png", dpi=150)
plt.close()

# Barras rutas
pr = top_rutas.toPandas()
plt.figure(figsize=(10,6))
plt.barh(
    y=[f"{r.estacion_origen}->{r.estacion_destino}" for r in pr.itertuples()],
    width=pr["count"]
)
plt.title("Top 20 rutas con mayor congestión")
plt.xlabel("N.º de viajes")
plt.tight_layout()
plt.savefig(RESULTS/"top_rutas.png", dpi=150)
plt.close()

spark.stop()
