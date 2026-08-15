"""Silver — entidad `geolocation`: una fila por código postal.

El origen trae 1 000 163 observaciones para ~19 000 códigos postales. Silver
consolida cada uno en una ubicación representativa:

- Coordenadas: **mediana**, no promedio. Una sola coordenada mal geocodificada
  desplazaría el promedio de todo el código postal; la mediana la ignora.
- Ciudad y estado: el valor **más frecuente** dentro del código postal.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from marketplace_dp.common.silver import clean_code, clean_text, read_bronze


def build_geolocation(spark: SparkSession, ingestion_date: date) -> DataFrame:
    geo = read_bronze(spark, "olist_geolocation_dataset", ingestion_date)

    typed = geo.select(
        F.col("geolocation_zip_code_prefix").cast("string").alias("zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("lat"),
        F.col("geolocation_lng").cast("double").alias("lng"),
        clean_text("geolocation_city").alias("city"),
        clean_code("geolocation_state").alias("state"),
    )

    coordinates = typed.groupBy("zip_code_prefix").agg(
        F.percentile_approx("lat", 0.5).alias("latitude"),
        F.percentile_approx("lng", 0.5).alias("longitude"),
    )

    # Moda de (ciudad, estado): contar cada combinación y quedarse con la primera
    # al ordenar por frecuencia. El desempate por nombre hace el resultado
    # DETERMINISTA: sin él, dos ejecuciones podrían elegir valores distintos.
    ranked = Window.partitionBy("zip_code_prefix").orderBy(
        F.desc("occurrences"), F.asc("city"), F.asc("state")
    )

    location = (
        typed.groupBy("zip_code_prefix", "city", "state")
        .agg(F.count("*").alias("occurrences"))
        .withColumn("rank", F.row_number().over(ranked))
        .filter(F.col("rank") == 1)
        .select("zip_code_prefix", "city", "state")
    )

    return coordinates.join(location, on="zip_code_prefix", how="left")
