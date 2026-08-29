"""Gold — `fct_reviews`: una fila por par (reseña, orden).

`review_id` NO es único: 814 identificadores están asociados a varias órdenes
distintas, de modo que la clave es la combinación `(review_id, order_id)`.

⚠️ Consecuencia para quien consulte esta tabla: una reseña que cubre dos órdenes
aparece en DOS filas. Al calcular la puntuación media hay que declarar si el grano
de análisis es la reseña o la orden — los dos números son distintos y ambos
legítimos según lo que se quiera medir.

Responde a BQ-03 (impacto del retraso en la satisfacción).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.facts import date_key, resolve_key
from marketplace_dp.common.lakehouse import read_gold, read_silver

# Umbrales de la escala de 1 a 5. Se declaran como constantes porque son una
# convención de negocio, no un detalle técnico.
NEGATIVE_MAX_SCORE = 2
POSITIVE_MIN_SCORE = 4

SECONDS_PER_HOUR = 3600


def build_fct_reviews(spark: SparkSession) -> DataFrame:
    reviews = read_silver(spark, "reviews")

    orders = read_silver(spark, "orders").select("order_id", "customer_unique_id")
    dim_customer = read_gold(spark, "dim_customer").select("customer_unique_id", "customer_sk")

    enriched = (
        reviews.join(orders, on="order_id", how="left")
        .join(dim_customer, on="customer_unique_id", how="left")
        # La fecha del hecho es cuándo se CREÓ la reseña, no cuándo se compró:
        # el proceso de negocio que esta tabla mide es "el cliente califica".
        .withColumn("date_sk", date_key("review_creation_date"))
        .withColumn("customer_sk", resolve_key("customer_sk"))
    )

    return enriched.select(
        # ─── Claves foráneas ─────────────────────────────────────────────
        "date_sk",
        "customer_sk",
        # ─── Dimensiones degeneradas ─────────────────────────────────────
        "review_id",
        "order_id",
        # ─── Medidas ─────────────────────────────────────────────────────
        # No aditiva: la puntuación se promedia, nunca se suma.
        F.col("review_score"),
        # Banderas 0/1 SÍ aditivas: permiten calcular el porcentaje como
        # SUM(is_negative) / COUNT(*) después de agregar, que sale correcto a
        # cualquier nivel. Almacenar el porcentaje daría promedios de promedios.
        (F.col("review_score") <= NEGATIVE_MAX_SCORE).cast("int").alias("is_negative"),
        (F.col("review_score") >= POSITIVE_MIN_SCORE).cast("int").alias("is_positive"),
        # Horas que tardó el vendedor en responder. No aditiva: se promedia.
        (
            (F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date"))
            / SECONDS_PER_HOUR
        )
        .cast("decimal(10, 2)")
        .alias("response_time_hours"),
        F.lit(1).alias("review_count"),
    )
