"""Gold — `fct_payments`: una fila por pago realizado sobre una orden.

Permanece SEPARADA de `fct_order_items`. Una orden con 3 ítems pagada en 2 plazos
generaría 6 filas al unirlas y duplicaría los ingresos: es el *fan trap* clásico.
El cruce entre ambos hechos se resuelve agregando cada uno a grano de orden antes
de unirlos (*drill-across*).

Responde a BQ-01 (mezcla de métodos de pago y financiación).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.facts import date_key, resolve_key
from marketplace_dp.common.lakehouse import read_gold, read_silver


def build_fct_payments(spark: SparkSession) -> DataFrame:
    payments = read_silver(spark, "payments")

    # `payments` solo tiene `order_id`: la fecha y el cliente hay que traerlos de
    # `orders`, que actúa de puente. La relación es 1:N (una orden, varios pagos),
    # así que el join NO duplica filas.
    orders = read_silver(spark, "orders").select(
        "order_id", "order_purchase_timestamp", "customer_unique_id"
    )
    dim_customer = read_gold(spark, "dim_customer").select("customer_unique_id", "customer_sk")

    enriched = (
        payments.join(orders, on="order_id", how="left")
        .join(dim_customer, on="customer_unique_id", how="left")
        .withColumn("date_sk", date_key("order_purchase_timestamp"))
        .withColumn("customer_sk", resolve_key("customer_sk"))
    )

    # SELECT explícito: fija el contrato de la tabla y evita que una columna nueva
    # en Silver se cuele sola en Gold.
    return enriched.select(
        # ─── Claves foráneas ─────────────────────────────────────────────
        "date_sk",
        "customer_sk",
        # ─── Dimensiones degeneradas ─────────────────────────────────────
        "order_id",
        "payment_sequential",
        # ─── Atributo ────────────────────────────────────────────────────
        "payment_type",
        # ─── Medidas ─────────────────────────────────────────────────────
        F.col("payment_value"),
        # No aditiva: el número de plazos se promedia, nunca se suma.
        F.col("payment_installments"),
        # Constante 1: permite contar pagos con SUM a cualquier nivel de
        # agregación, igual que cualquier otra medida aditiva.
        F.lit(1).alias("payment_count"),
    )
