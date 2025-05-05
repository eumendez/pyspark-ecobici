"""
Definición de mapeo de columnas y esquemas Spark.
"""

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType
)

COLUMN_MAP = {
    "estacion_origen": [
        "Ciclo_Estacion_Retiro", "ciclo_estacion_retiro",
        "ciclo_estacion_retir", "station_id_r", "estacion_origen"
    ],
    "estacion_destino": [
        "Ciclo_Estacion_Arribo", "Ciclo_EstacionArribo",
        "ciclo_estacion_arribo", "station_id_a", "estacion_destino"
    ],
    "fecha_retiro": ["Fecha_Retiro", "fecha_retiro"],
    "hora_retiro":  ["Hora_Retiro",  "hora_retiro"],
    "fecha_arribo": ["Fecha_Arribo", "Fecha Arribo", "fecha_arribo"],
    "hora_arribo":  ["Hora_Arribo",  "hora_arribo"],
    "genero":       ["Genero_Usuario", "genero_usuario", "sexo", "gender"],
    "edad_rango":   ["Edad_Usuario",   "edad_usuario",  "rango_edad", "age_range"],
    "id_bici":      ["Bici", "bici", "bike_id"]
}

def get_standard_name(name: str) -> str:
    """Devuelve la clave estándar para un alias dado (solo para tests)."""
    for std, aliases in COLUMN_MAP.items():
        if name in aliases:
            return std
    return name

NORMALIZED_SCHEMA = StructType([
    StructField("estacion_origen", IntegerType(), True),
    StructField("estacion_destino", IntegerType(), True),
    StructField("fecha_hora_retiro", TimestampType(), True),
    StructField("fecha_hora_arribo", TimestampType(), True),
    StructField("genero", StringType(), True),
    StructField("edad_rango", StringType(), True),
    StructField("id_bici", IntegerType(), True),
    StructField("duracion_segundos", IntegerType(), True)
])
