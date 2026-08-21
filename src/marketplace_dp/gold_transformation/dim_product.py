from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.dimensions import with_unknown_member
from marketplace_dp.common.lakehouse import read_silver


def build_dim_product(spark: SparkSession) -> DataFrame:
    """Construye la dimensión de productos."""

    dim_product = read_silver(spark, "products")

    dim_product = dim_product.withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"),
    )

    dim_product = dim_product.withColumn(
        "weight_bucket",
        F.when(F.col("product_weight_g") < 500, "ligero")
        .when((F.col("product_weight_g") >= 500) & (F.col("product_weight_g") <= 2000), "medio")
        .when(F.col("product_weight_g") > 2000, "pesado"),
    )

    dim_product = dim_product.withColumn("product_sk", F.sha2(F.col("product_id"), 256))

    return with_unknown_member(dim_product, surrogate_key="product_sk")
