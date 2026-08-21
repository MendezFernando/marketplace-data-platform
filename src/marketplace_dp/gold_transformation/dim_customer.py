from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.dimensions import with_unknown_member
from marketplace_dp.common.lakehouse import read_silver


def build_dim_customer(spark: SparkSession) -> DataFrame:
    """Construye la dimensión de clientes."""

    dim_customers = read_silver(spark, "customers")
    orders = read_silver(spark, "orders")

    first_purchase = (
        orders.groupBy("customer_unique_id")
        .agg(F.min("order_purchase_timestamp").alias("first_purchase_date"))
        .withColumn("cohort_year_month", F.date_format("first_purchase_date", "yyyy-MM"))
    )

    dim_customer = dim_customers.join(first_purchase, on="customer_unique_id", how="left")

    dim_customer = dim_customer.withColumn("customer_sk", F.sha2(F.col("customer_unique_id"), 256))

    return with_unknown_member(dim_customer, surrogate_key="customer_sk")
