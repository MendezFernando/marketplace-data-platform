"""Silver — entidad `orders`: una fila por orden.

Se mantiene separada de `order_items` porque los granos son distintos y porque
775 órdenes no tienen ítems asociados (canceladas o sin disponibilidad):
fusionarlas con un INNER JOIN las eliminaría e incumpliría el SLA-02.

Esta entidad materializa además la resolución de identidad del cliente: arrastra
`customer_unique_id` para que `orders` y `customers` puedan unirse (BQ-02).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.lakehouse import clean_code, read_bronze, to_timestamp

DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def build_orders(spark: SparkSession, ingestion_date: date) -> DataFrame:
    orders = read_bronze(spark, "olist_orders_dataset", ingestion_date)
    customers = read_bronze(spark, "olist_customers_dataset", ingestion_date)

    # Puente de identidad: customer_id (una por orden) → customer_unique_id (persona).
    # Verificado que la relación es 1:1 hacia la persona, así que no duplica filas.
    identity = customers.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("customer_unique_id").cast("string").alias("customer_unique_id"),
    ).dropDuplicates(["customer_id"])

    typed = orders.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        clean_code("order_status").alias("order_status"),
        *[to_timestamp(c).alias(c) for c in DATE_COLUMNS],
    )

    return typed.join(identity, on="customer_id", how="left").select(
        "order_id",
        "customer_id",
        "customer_unique_id",
        "order_status",
        *DATE_COLUMNS,
    )
