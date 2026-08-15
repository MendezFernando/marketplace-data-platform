"""Silver — entidad `order_items`: una fila por producto vendido dentro de una orden.

Es el grano atómico de la venta y el único nivel donde `price` y `freight_value`
son aditivos. No se fusiona con `orders`: son hechos conformados que comparten
`order_id`.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.silver import read_bronze, to_money, to_timestamp


def build_order_items(spark: SparkSession, ingestion_date: date) -> DataFrame:
    items = read_bronze(spark, "olist_order_items_dataset", ingestion_date)

    return items.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("seller_id").cast("string").alias("seller_id"),
        to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        to_money("price").alias("price"),
        to_money("freight_value").alias("freight_value"),
    )
