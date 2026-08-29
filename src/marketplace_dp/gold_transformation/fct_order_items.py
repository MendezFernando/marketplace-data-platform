"""Gold — `fct_order_items`: una fila por producto vendido dentro de una orden.

Es el grano atómico de la venta y el único nivel donde `price` y `freight_value`
son aditivos. No se fusiona con `fct_orders`: son hechos conformados que comparten
`order_id` y se cruzan agregando cada uno a un grano común (*drill-across*).

Es la única tabla de hechos con un **join temporal**: `dim_seller` usa SCD Tipo 2,
así que cada venta debe unirse con la versión del vendedor vigente **en la fecha de
compra**, no con la versión actual. Ahí es donde BQ-04 se vuelve resoluble.

Responde a BQ-01 (margen por categoría) y BQ-04 (GMV por segmento de vendedor).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.facts import date_key, resolve_key
from marketplace_dp.common.lakehouse import read_gold, read_silver


def build_fct_order_items(spark: SparkSession) -> DataFrame:
    items = read_silver(spark, "order_items")

    # `order_items` no tiene fecha ni cliente: los trae `orders`, que actúa de
    # puente. La relación es 1:N (una orden, varios ítems), así que no duplica.
    orders = read_silver(spark, "orders").select(
        "order_id", "order_purchase_timestamp", "customer_unique_id"
    )

    dim_customer = read_gold(spark, "dim_customer").select(
        "customer_unique_id", "customer_sk", "customer_zip_code_prefix"
    )

    dim_geography = read_gold(spark, "dim_geography").select("zip_code_prefix", "geography_sk")

    dim_product = read_gold(spark, "dim_product").select("product_id", "product_sk")

    # La clave natural de la dimensión se RENOMBRA: si ambos lados del join tienen
    # una columna `seller_id`, Spark no sabe a cuál se refiere cada expresión y
    # falla por ambigüedad.
    dim_seller = read_gold(spark, "dim_seller").select(
        F.col("seller_id").alias("dim_seller_id"),
        F.col("seller_sk"),
        F.col("valid_from"),
        F.col("valid_to"),
    )

    enriched = (
        items.join(orders, on="order_id", how="left")
        # La fecha de compra en tipo DATE: `valid_from` y `valid_to` son DATE, y
        # comparar un TIMESTAMP contra un DATE produce resultados sutilmente
        # equivocados en los bordes de la vigencia.
        .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
        .join(dim_customer, on="customer_unique_id", how="left")
        .join(
            dim_geography,
            F.col("customer_zip_code_prefix") == F.col("zip_code_prefix"),
            how="left",
        )
        .join(dim_product, on="product_id", how="left")
        # ─── El join temporal (point-in-time) ────────────────────────────
        # Unir solo por seller_id duplicaría cada venta una vez por versión del
        # vendedor. La condición de rango selecciona EXACTAMENTE UNA versión,
        # porque las vigencias de dim_seller no se solapan ni dejan huecos.
        .join(
            dim_seller,
            (F.col("seller_id") == F.col("dim_seller_id"))
            & F.col("purchase_date").between(F.col("valid_from"), F.col("valid_to")),
            how="left",
        )
    )

    return enriched.select(
        # ─── Claves foráneas ─────────────────────────────────────────────
        date_key("order_purchase_timestamp").alias("date_sk"),
        resolve_key("customer_sk").alias("customer_sk"),
        resolve_key("seller_sk").alias("seller_sk"),
        resolve_key("product_sk").alias("product_sk"),
        resolve_key("geography_sk").alias("geography_sk"),
        # ─── Dimensiones degeneradas ─────────────────────────────────────
        "order_id",
        "order_item_id",
        # ─── Medidas: todas aditivas ─────────────────────────────────────
        F.col("price"),
        F.col("freight_value"),
        (F.col("price") + F.col("freight_value")).alias("gross_revenue"),
        F.lit(1).alias("item_count"),
    )
