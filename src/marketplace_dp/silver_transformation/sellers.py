"""Silver — entidad `sellers`: una fila por vendedor.

Es la entidad que deberá convertirse en SCD Tipo 2 cuando se implementen los
segmentos de vendedor (BQ-04), ya que exige atribuir cada venta al segmento
vigente en la fecha de compra.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.silver import clean_code, clean_text, read_bronze


def build_sellers(spark: SparkSession, ingestion_date: date) -> DataFrame:
    sellers = read_bronze(spark, "olist_sellers_dataset", ingestion_date)

    return sellers.select(
        F.col("seller_id").cast("string").alias("seller_id"),
        # El código postal se conserva como TEXTO: castearlo a entero destruiría
        # los ceros iniciales ("01001" → 1001).
        F.col("seller_zip_code_prefix").cast("string").alias("seller_zip_code_prefix"),
        clean_text("seller_city").alias("seller_city"),
        clean_code("seller_state").alias("seller_state"),
    )
