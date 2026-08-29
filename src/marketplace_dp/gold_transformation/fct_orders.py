"""Gold — `fct_orders`: una fila por orden.

Es un **snapshot acumulativo**: guarda las fechas de los hitos del ciclo de vida
(compra → aprobación → transportista → entrega) en la misma fila, que se van
rellenando conforme el proceso avanza. Eso permite calcular duraciones entre
hitos sin ningún join.

Se mantiene SEPARADA de `fct_order_items` porque los granos son distintos y
porque 775 órdenes no tienen ítems asociados: fusionarlas con un INNER JOIN las
eliminaría e incumpliría el SLA-02 de completitud.

Responde a BQ-00 (entregas tardías), BQ-02 (cohortes) y BQ-03 (satisfacción).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.facts import date_key, resolve_key
from marketplace_dp.common.lakehouse import read_gold, read_silver

DELIVERED_STATUS = "DELIVERED"


def build_fct_orders(spark: SparkSession) -> DataFrame:
    orders = read_silver(spark, "orders")

    dim_customer = read_gold(spark, "dim_customer").select(
        "customer_unique_id", "customer_sk", "customer_zip_code_prefix"
    )
    dim_geography = read_gold(spark, "dim_geography").select("zip_code_prefix", "geography_sk")

    # Medidas agregadas desde el grano de ítem hasta el grano de orden.
    # Este es el paso de *drill-across*: se agrega ANTES de unir, nunca después.
    items = (
        read_silver(spark, "order_items")
        .groupBy("order_id")
        .agg(
            F.count("*").alias("order_item_count"),
            F.sum(F.col("price") + F.col("freight_value")).alias("order_value"),
        )
    )

    enriched = (
        orders.join(dim_customer, on="customer_unique_id", how="left")
        # El código postal del cliente da la geografía de entrega.
        .join(
            dim_geography,
            orders["customer_unique_id"].isNotNull()
            & (F.col("customer_zip_code_prefix") == F.col("zip_code_prefix")),
            how="left",
        )
        # LEFT JOIN: las 775 órdenes sin ítems conservan su fila con las medidas
        # a NULL, que el coalesce convierte en 0.
        .join(items, on="order_id", how="left")
    )

    return enriched.select(
        # ─── Claves foráneas ─────────────────────────────────────────────
        # Las tres apuntan a la MISMA dim_date desempeñando papeles distintos:
        # se llama *role-playing dimension*. No se duplica la tabla; en la
        # herramienta de BI se crean tres alias.
        date_key("order_purchase_timestamp").alias("purchase_date_sk"),
        date_key("order_delivered_customer_date").alias("delivered_date_sk"),
        date_key("order_estimated_delivery_date").alias("estimated_date_sk"),
        resolve_key("customer_sk").alias("customer_sk"),
        resolve_key("geography_sk").alias("geography_sk"),
        # ─── Dimensión degenerada ────────────────────────────────────────
        "order_id",
        # ─── Atributo ────────────────────────────────────────────────────
        # Con 8 valores y ningún atributo que describir, una dim_order_status
        # solo añadiría un join sin aportar información.
        "order_status",
        # ─── Medidas ─────────────────────────────────────────────────────
        # NO ADITIVAS: sumar días de entrega de 100 órdenes no significa nada.
        # Se guardan porque el promedio por grupo sí es una métrica válida.
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
        .cast("int")
        .alias("days_to_delivery"),
        # Negativo si llegó ANTES de lo prometido. Al promediarlo hay que filtrar
        # por is_late = 1, o los adelantos cancelarían a los retrasos.
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
        .cast("int")
        .alias("days_late"),
        # ADITIVAS: banderas 0/1 que permiten
        #   % tardías = SUM(is_late) / SUM(is_delivered)
        # calculado DESPUÉS de agregar, correcto a cualquier nivel.
        (F.col("order_status") == DELIVERED_STATUS).cast("int").alias("is_delivered"),
        F.coalesce(
            (F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")).cast(
                "int"
            ),
            F.lit(0),
        ).alias("is_late"),
        F.coalesce(F.col("order_item_count"), F.lit(0)).alias("order_item_count"),
        F.coalesce(F.col("order_value"), F.lit(0).cast("decimal(12, 2)")).alias("order_value"),
        F.lit(1).alias("order_count"),
    )
