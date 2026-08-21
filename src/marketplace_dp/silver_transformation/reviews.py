"""Silver — entidad `reviews`: una fila por relación entre reseña y orden.

`review_id` NO es único: 814 identificadores aparecen asociados a varias órdenes
distintas (misma puntuación, misma fecha, pedidos diferentes), probablemente
porque una única encuesta cubría varios pedidos. No existe ningún duplicado
exacto, de modo que la clave es la combinación `(review_id, order_id)`.

⚠️ Consecuencia para Gold: al unir `reviews` con `orders`, una reseña que cubre
dos órdenes cuenta dos veces. Toda métrica de puntuación media debe declarar si
su grano de análisis es la reseña o la orden.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.lakehouse import read_bronze, to_timestamp


def build_reviews(spark: SparkSession, ingestion_date: date) -> DataFrame:
    reviews = read_bronze(spark, "olist_order_reviews_dataset", ingestion_date)

    typed = reviews.select(
        F.col("review_id").cast("string").alias("review_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.col("review_score").cast("int").alias("review_score"),
        # El texto libre se conserva ÍNTEGRO: 3 852 comentarios contienen saltos
        # de línea embebidos y son parte legítima del dato. Solo se normaliza el
        # vacío a NULL.
        F.when(F.trim(F.col("review_comment_title")) == "", None)
        .otherwise(F.col("review_comment_title"))
        .alias("review_comment_title"),
        F.when(F.trim(F.col("review_comment_message")) == "", None)
        .otherwise(F.col("review_comment_message"))
        .alias("review_comment_message"),
        to_timestamp("review_creation_date").alias("review_creation_date"),
        to_timestamp("review_answer_timestamp").alias("review_answer_timestamp"),
    )

    # Duplicados EXACTOS de (review_id, order_id) no deberían existir, pero
    # eliminarlos aquí hace la entidad robusta ante una reingesta parcial.
    return typed.dropDuplicates(["review_id", "order_id"])
