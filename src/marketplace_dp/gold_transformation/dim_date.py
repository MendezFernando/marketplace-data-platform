from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.dimensions import with_unknown_member

start_date = "2016-01-01"
end_date = "2020-12-31"


def build_dim_date(spark: SparkSession) -> DataFrame:
    """Construye la dimensión de fechas."""

    # Generar una fila por cada fecha.
    dim_date = spark.range(1).select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date)),
                F.to_date(F.lit(end_date)),
                F.expr("interval 1 day"),
            )
        ).alias("full_date")
    )

    # Crear atributos de la fecha.
    dim_date = (
        dim_date.withColumn(
            "date_sk",
            F.date_format("full_date", "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "year",
            F.year("full_date"),
        )
        .withColumn(
            "quarter",
            F.quarter("full_date"),
        )
        .withColumn(
            "month",
            F.month("full_date"),
        )
        .withColumn(
            "month_name",
            F.date_format("full_date", "MMMM"),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("full_date"),
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("full_date"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("full_date"),
        )
        .withColumn(
            "day_name",
            F.date_format("full_date", "EEEE"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin([1, 7]),
        )
        .withColumn(
            "year_month",
            F.date_format("full_date", "yyyy-MM"),
        )
        .withColumn(
            "year_quarter",
            F.concat(
                F.year("full_date"),
                F.lit("-Q"),
                F.quarter("full_date"),
            ),
        )
    )

    # La fila del miembro desconocido se deriva del propio esquema: ya no hace
    # falta declarar un StructType ni mantenerlo sincronizado al añadir columnas.
    return with_unknown_member(dim_date, surrogate_key="date_sk")
