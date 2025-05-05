"""
Pipeline de limpieza y features para datos Ecobici.
"""

import re
from pyspark.sql import DataFrame, functions as F
from schema import COLUMN_MAP

# ------------------------------------------------------------------ #
# A) Renombrado eficiente                                             #
# ------------------------------------------------------------------ #

def _build_rename_expr(df: DataFrame):
    exprs = []
    for std, aliases in COLUMN_MAP.items():
        for alias in aliases:
            if alias in df.columns:
                exprs.append(F.col(alias).alias(std))
                break
    return exprs

def rename_columns(df: DataFrame) -> DataFrame:
    """
    Renombra las columnas del DataFrame de acuerdo al diccionario COLUMN_MAP.
    
    Args:
        df (DataFrame): DataFrame original con nombres inconsistentes.
    
    Returns:
        DataFrame: DataFrame con nombres de columnas estandarizados.
    """
    return df.select(*_build_rename_expr(df))

# ------------------------------------------------------------------ #
# B) Corrección de fechas y horas                                     #
# ------------------------------------------------------------------ #

_DATE_RE = r"^\d{1,4}([/-])\d{1,2}\1\d{1,4}$"

@F.udf("string")
def fix_date(fecha: str) -> str:
    """
    Convierte fechas en formato 'DD/MM/YYYY' o 'YYYY-MM-DD' a 'YYYY-MM-DD'.

    Args:
        fecha (str): Fecha en texto.

    Returns:
        str: Fecha en formato unificado 'YYYY-MM-DD' o None si no es válida.
    """
    if fecha is None:
        return None
    m = re.match(_DATE_RE, fecha)
    if not m:
        return None
    sep = m.group(1)
    parts = fecha.split(sep)
    if len(parts[0]) == 4:
        y, mth, d = parts
    else:
        d, mth, y = parts
    return f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"

_TIME_RE = r"^(?P<h>\d{1,2}):(?P<m>\d{1,2}):(?P<s>\d{1,2})(?:\.\d+)?$"

@F.udf("string")
def fix_time(hora: str) -> str:
    """
    Corrige horas inválidas aplicando módulo 24/60/60.
    Acepta formatos con microsegundos.

    Args:
        hora (str): Hora en formato 'HH:MM:SS' o con fracción.

    Returns:
        str: Hora corregida en formato 'HH:MM:SS' o None si no es válida.
    """
    if hora is None:
        return None
    m = re.match(_TIME_RE, hora)
    if not m:
        return None
    h = int(m["h"]) % 24
    mnt = int(m["m"]) % 60
    s = int(m["s"]) % 60
    return f"{h:02d}:{mnt:02d}:{s:02d}"

def apply_date_time_fix(df: DataFrame) -> DataFrame:
    """
    Aplica correcciones a columnas de fecha y hora usando fix_date y fix_time.

    Args:
        df (DataFrame): DataFrame con columnas 'fecha_*' y 'hora_*'.

    Returns:
        DataFrame: DataFrame con fechas y horas corregidas.
    """
    for c in ("fecha_retiro", "fecha_arribo"):
        if c in df.columns:
            df = df.withColumn(c, fix_date(F.col(c)))
    for c in ("hora_retiro", "hora_arribo"):
        if c in df.columns:
            df = df.withColumn(c, fix_time(F.col(c)))
    return df

# ------------------------------------------------------------------ #
# C) Unificación de fecha y hora                                     #
# ------------------------------------------------------------------ #

def merge_datetime(df: DataFrame) -> DataFrame:
    """
    Une columnas de fecha y hora en columnas timestamp.
    Filtra registros inválidos.

    Args:
        df (DataFrame): DataFrame con columnas 'fecha_*' y 'hora_*'.

    Returns:
        DataFrame: DataFrame con 'fecha_hora_retiro' y 'fecha_hora_arribo'.
    """
    fmt = "yyyy-MM-dd HH:mm:ss"
    df = df.withColumn(
        "fecha_hora_retiro",
        F.to_timestamp(F.concat_ws(" ", "fecha_retiro", "hora_retiro"), fmt)
    ).withColumn(
        "fecha_hora_arribo",
        F.to_timestamp(F.concat_ws(" ", "fecha_arribo", "hora_arribo"), fmt)
    )

    df = df.drop("fecha_retiro", "hora_retiro", "fecha_arribo", "hora_arribo")

    return df.filter(
        F.col("fecha_hora_retiro").isNotNull() &
        F.col("fecha_hora_arribo").isNotNull()
    )

# ------------------------------------------------------------------ #
# D) Manejo de valores nulos                                         #
# ------------------------------------------------------------------ #

def fill_nulls(df: DataFrame) -> DataFrame:
    """
    Rellena valores nulos según el tipo de dato:
      -1 para enteros, 'DESCONOCIDO' para cadenas.

    Args:
        df (DataFrame): DataFrame con posibles nulos.

    Returns:
        DataFrame: DataFrame sin nulos.
    """
    enteros = ["estacion_origen", "estacion_destino", "id_bici"]
    cadenas = ["genero", "edad_rango"]
    df = df.fillna(-1, subset=enteros).fillna("DESCONOCIDO", subset=cadenas)
    return df

# ------------------------------------------------------------------ #
# E) Cálculo de duración del viaje                                   #
# ------------------------------------------------------------------ #

def add_duration(df: DataFrame) -> DataFrame:
    """
    Calcula duración del viaje en segundos.
    Filtra viajes negativos o > 24 horas.

    Args:
        df (DataFrame): DataFrame con timestamps de arribo y retiro.

    Returns:
        DataFrame: DataFrame con columna 'duracion_segundos'.
    """
    df = df.withColumn(
        "duracion_segundos",
        F.unix_timestamp("fecha_hora_arribo") - F.unix_timestamp("fecha_hora_retiro")
    ).filter(
        (F.col("duracion_segundos") >= 0) & (F.col("duracion_segundos") <= 86400)
    )
    return df

# ------------------------------------------------------------------ #
# F) Conversión de tipos                                             #
# ------------------------------------------------------------------ #
def cast_types(df: DataFrame) -> DataFrame:
    """
    Convierte columnas numéricas a IntegerType de forma segura.
    Cualquier valor no numérico queda en null y luego se rellena a -1.
    """
    int_cols = ["estacion_origen", "estacion_destino", "id_bici", "duracion_segundos"]
    for c in int_cols:
        df = df.withColumn(
            c,
            F.regexp_replace(F.col(c).cast("string"), r"[^0-9]", "").cast("int")
        )
    return df

# ------------------------------------------------------------------ #
# G) Pipeline completa                                               #
# ------------------------------------------------------------------ #
def clean_pipeline(df: DataFrame) -> DataFrame:
    """
    Ejecuta toda la secuencia de limpieza de datos.
    """
    return (
        df.transform(rename_columns)
          .transform(apply_date_time_fix)
          .transform(merge_datetime)
          .transform(fill_nulls)
          .transform(add_duration)
          .transform(cast_types)
    )
