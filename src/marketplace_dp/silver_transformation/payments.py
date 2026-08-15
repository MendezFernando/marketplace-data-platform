"""Silver — entidad `payments`: una fila por pago realizado sobre una orden.

Permanece independiente de `order_items`. Una orden puede pagarse con varios
métodos (tarjeta + voucher); unir ambos hechos produciría un producto cartesiano
—3 ítems × 2 pagos = 6 filas— y duplicaría los ingresos. Es el *fan trap* clásico.
El cruce entre ambos se resuelve agregando cada uno a un grano común antes de
unirlos (*drill-across*).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.silver import clean_text, read_bronze, to_money


def build_payments(spark: SparkSession, ingestion_date: date) -> DataFrame:
    payments = read_bronze(spark, "olist_order_payments_dataset", ingestion_date)

    return payments.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        clean_text("payment_type").alias("payment_type"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        to_money("payment_value").alias("payment_value"),
    )
