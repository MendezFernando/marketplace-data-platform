"""Silver — entidad `products`: una fila por producto.

La tabla de traducciones (71 filas) es un catálogo de referencia, no una entidad
de negocio: se integra aquí conservando el nombre original en portugués y su
traducción al inglés.

Las columnas de longitud de nombre y descripción se conservan aunque ningún
requisito actual las use: Silver debe mantenerse neutral respecto a casos de uso
futuros, y son predictores plausibles de conversión.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.silver import clean_text, read_bronze


def build_products(spark: SparkSession, ingestion_date: date) -> DataFrame:
    products = read_bronze(spark, "olist_products_dataset", ingestion_date)
    translation = read_bronze(spark, "product_category_name_translation", ingestion_date)

    typed = products.select(
        F.col("product_id").cast("string").alias("product_id"),
        clean_text("product_category_name").alias("product_category_name"),
        # El origen escribe "lenght" (error tipográfico del sistema fuente).
        # Bronze lo conserva tal cual por fidelidad; Silver lo corrige.
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("int").alias("product_weight_g"),
        F.col("product_length_cm").cast("int").alias("product_length_cm"),
        F.col("product_height_cm").cast("int").alias("product_height_cm"),
        F.col("product_width_cm").cast("int").alias("product_width_cm"),
    )

    categories = translation.select(
        clean_text("product_category_name").alias("product_category_name"),
        clean_text("product_category_name_english").alias("product_category_name_english"),
    ).dropDuplicates(["product_category_name"])

    # LEFT JOIN, no INNER: hay productos cuya categoría no figura en el catálogo
    # de traducción, y perderlos falsearía el conteo de productos.
    return typed.join(categories, on="product_category_name", how="left").select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )
