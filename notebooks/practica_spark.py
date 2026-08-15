"""Laboratorio de PySpark — ejecútalo por bloques y observa la salida.

    python notebooks/practica_spark.py

Cámbialo, rómpelo, vuelve a ejecutarlo. Está aquí para experimentar,
no para producción (por eso vive en notebooks/ y no en src/).
"""

# from pyspark.sql import Window
# from pyspark.sql import functions as F

# from marketplace_dp.common.spark import get_spark

# spark = get_spark("practica")

# ─────────────────────────────────────────────────────────────────────────────
#  Ojo: leemos UNA partición, no la carpeta entera.
#  Leer "s3a://bronze/olist/olist_geolocation_dataset/" daría la unión de todos
#  los días de ingesta.
# ─────────────────────────────────────────────────────────────────────────────
# geo = spark.read.parquet(
#     "s3a://bronze/olist/olist_geolocation_dataset/ingestion_date=2026-08-01/"
# )

# print("\n" + "=" * 70)
# print("1. EXPLORAR")
# print("=" * 70)
# geo.printSchema()
# geo.show(5, truncate=False)
# print("filas:", geo.count())


# print("\n" + "=" * 70)
# print("2. SELECCIONAR Y FILTRAR")
# print("=" * 70)
# (
#     geo.select("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state")
#     .filter(F.col("geolocation_state") == "SP")
#     .show(5)
# )


# print("\n" + "=" * 70)
# print("4. AGREGAR — cuántos ZIP distintos hay por estado")
# print("=" * 70)
# (
#     tipado.groupBy("estado")
#     .agg(
#         F.count("*").alias("filas"),
#         F.countDistinct("geolocation_zip_code_prefix").alias("zips"),
#     )
#     .orderBy(F.desc("filas"))
#     .show(10)
# )


# print("\n" + "=" * 70)
# print("5. MEDIANA POR ZIP — aquí se produce un SHUFFLE")
# print("=" * 70)
# mediana = tipado.groupBy("geolocation_zip_code_prefix").agg(
#     F.percentile_approx("lat", 0.5).alias("lat"),
#     F.percentile_approx("lng", 0.5).alias("lng"),
# )
# print("ZIP distintos:", mediana.count())
# mediana.show(5)


# print("\n" + "=" * 70)
# print("6. VENTANA — la ciudad más frecuente de cada ZIP (moda)")
# print("=" * 70)
# w = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("n"), F.asc("ciudad"))
# moda = (
#     tipado.groupBy("geolocation_zip_code_prefix", "ciudad")
#     .agg(F.count("*").alias("n"))
#     .withColumn("rn", F.row_number().over(w))
#     .filter(F.col("rn") == 1)
#     .select("geolocation_zip_code_prefix", "ciudad")
# )
# moda.show(5)


# print("\n" + "=" * 70)
# print("7. JOIN — unir mediana con moda")
# print("=" * 70)
# resultado = mediana.join(moda, on="geolocation_zip_code_prefix", how="left")
# resultado.show(5)
# print("filas finales:", resultado.count())


# print("\n" + "=" * 70)
# print("8. left_anti — detectar integridad referencial")
# print("=" * 70)
# sin_moda = mediana.join(moda, on="geolocation_zip_code_prefix", how="left_anti")
# print("ZIP sin ciudad resuelta:", sin_moda.count())


# ─────────────────────────────────────────────────────────────────────────────
#  EJERCICIOS — descoméntalos y complétalos
# ─────────────────────────────────────────────────────────────────────────────
# A) ¿Cuántos ZIP distintos hay en total? ¿Coincide con las filas de `mediana`?
# #
# print("=" * 70)
# geo.select(F.countDistinct("geolocation_zip_code_prefix")).show()
# # B) Compara la MEDIA y la MEDIANA de latitud para el estado "SP".
# #    ¿Cuánto se diferencian? ¿Qué te dice eso sobre los valores atípicos?
# #
# print("\n" + "=" * 70)
# print("3. TIPADO Y COLUMNAS NUEVAS")
# print("=" * 70)
# tipado = (
#     geo.withColumn("lat", F.col("geolocation_lat").cast("double"))
#     .withColumn("lng", F.col("geolocation_lng").cast("double"))
#     .withColumn("ciudad", F.trim(F.lower(F.col("geolocation_city"))))
#     .withColumn("estado", F.upper(F.trim(F.col("geolocation_state"))))
#     .withColumn(
#         "region",
#         F.when(F.col("geolocation_state").isin("SP", "RJ", "MG", "ES"), "Sudeste")
#         .when(F.col("geolocation_state").isin("RS", "SC", "PR"), "Sur")
#         .otherwise("Otra"),
#     )
# )
# tipado.select("ciudad", "estado", "region", "lat", "lng").show(5)


# print("\n" + "=" * 70)
# print("5. MEDIANA POR ZIP — aquí se produce un SHUFFLE")
# print("=" * 70)
# mediana = tipado.groupBy("geolocation_zip_code_prefix").agg(
#     F.percentile_approx("lat", 0.5).alias("lat_mediana"),
#     F.percentile_approx("lng", 0.5).alias("lng_mediana"),
# )
# print("ZIP distintos:", mediana.count())

# print("\n" + "=" * 70)
# print("5. MEDIA POR ZIP — aquí se produce un SHUFFLE")
# print("=" * 70)
# media = tipado.groupBy("geolocation_zip_code_prefix").agg(
#     F.mean("lat").alias("lat_media"),
#     F.mean("lng").alias("lng_media"),
# )
# print("ZIP distintos:", media.count())

# geo_dif_mediana_media = mediana.join(
#     media,
#     on="geolocation_zip_code_prefix",
#     how="inner"
# )

# geo_dif_mediana_media = geo_dif_mediana_media.withColumn(
#     "dif_lat", F.abs(F.col("lat_mediana") - F.col("lat_media"))
# ).withColumn("dif_lng", F.abs(F.col("lng_mediana") - F.col("lng_media")))


# geo_dif_mediana_media.show(20)
# # C) Añade el estado más frecuente por ZIP, igual que hiciste con la ciudad.


# #    ¿Puedes hacerlo sin repetir todo el bloque de la ventana?
# #
# # D) Valida que `geolocation_zip_code_prefix` es único en `resultado`.
# #    Pista: compara .count() con .select(...).distinct().count()
# #
# # E) Encuentra coordenadas imposibles para Brasil
# #    (lat fuera de [-34, 6] o lng fuera de [-74, -34]). ¿Cuántas hay?

# print("\n" + "=" * 70)
# print("E) Coordenadas imposibles para Brasil")
# print("=" * 70)
# tipado.filter(
#     ((F.col("lat") < -34) | (F.col("lat") > 6))
#     & ((F.col("lng") < -74) | (F.col("lng") > -34))
# ).count()

# spark.stop()
import pandas as pd

from marketplace_dp.common.config import settings

df = pd.read_parquet(
    "s3://bronze/olist/olist_geolocation_dataset/ingestion_date=2026-08-01/olist_geolocation_dataset.parquet",
    engine="pyarrow",
    storage_options={
        "key": settings.minio_root_user,
        "secret": settings.minio_root_password,
        "client_kwargs": {"endpoint_url": settings.minio_endpoint},
    },
)
print(df.columns)
