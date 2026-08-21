"""Silver — entidad `customers`: una fila por persona.

El origen genera un `customer_id` distinto por cada orden, de modo que no
identifica a una persona sino a su aparición en un pedido. La identidad real es
`customer_unique_id`: 99 441 registros corresponden a 96 096 personas.

Usar `customer_id` como clave haría que cada cliente comprara exactamente una vez
y la tasa de recompra de BQ-02 saldría 0% — un número plausible y falso.

**Resolución de conflictos:** 122 personas aparecen con más de una ciudad y 39 con
más de un estado. Se conserva el valor de la orden **más reciente**, porque la
ubicación describe dónde está el cliente hoy. Esta estrategia no preserva
historial; si el negocio necesitara atribuir ventas pasadas a la ubicación
vigente entonces, la entidad debería convertirse en SCD Tipo 2.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from marketplace_dp.common.lakehouse import clean_code, clean_text, read_bronze, to_timestamp


def build_customers(spark: SparkSession, ingestion_date: date) -> DataFrame:
    customers = read_bronze(spark, "olist_customers_dataset", ingestion_date)
    orders = read_bronze(spark, "olist_orders_dataset", ingestion_date)

    typed = customers.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("customer_unique_id").cast("string").alias("customer_unique_id"),
        F.col("customer_zip_code_prefix").cast("string").alias("customer_zip_code_prefix"),
        clean_text("customer_city").alias("customer_city"),
        clean_code("customer_state").alias("customer_state"),
    )

    # Se castea a TIMESTAMP antes de ordenar. Ordenar la cadena de texto
    # funcionaría por casualidad con el formato ISO, pero dejaría de hacerlo si
    # el origen cambiara de formato — y elegiría la orden equivocada en silencio.
    purchases = orders.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        to_timestamp("order_purchase_timestamp").alias("purchased_at"),
    )

    # LEFT JOIN: un cliente sin órdenes conserva su fila con `purchased_at` NULL.
    enriched = typed.join(purchases, on="customer_id", how="left")

    # Una fila por persona: la de su compra más reciente.
    # desc_nulls_last() es explícito a propósito: si una persona tuviera órdenes
    # con y sin fecha, queremos que gane la que sí la tiene.
    most_recent = Window.partitionBy("customer_unique_id").orderBy(
        F.col("purchased_at").desc_nulls_last()
    )

    return (
        enriched.withColumn("rank", F.row_number().over(most_recent))
        .filter(F.col("rank") == 1)
        .select(
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
    )
