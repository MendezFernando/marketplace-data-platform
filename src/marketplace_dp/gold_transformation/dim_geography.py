"""Gold — `dim_geography`: una fila por código postal.

SCD Tipo 1: la geografía no cambia, así que no se conserva historial.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.dimensions import with_unknown_member
from marketplace_dp.common.lakehouse import read_silver

# Las 27 unidades federativas de Brasil agrupadas en las 5 macrorregiones
# oficiales del IBGE. Analizar por 27 estados es inmanejable en un dashboard;
# por 5 regiones es la agrupación que usa el negocio en Brasil.
BRAZIL_REGIONS: dict[str, list[str]] = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}


def region_column(state_column: str) -> F.Column:
    """Traduce el código de estado a su macrorregión.

    Se construye encadenando `when` a partir del diccionario en lugar de
    escribirlos a mano: si mañana cambia la agrupación, se edita el diccionario
    y esta función no se toca.

    Un estado desconocido devuelve NULL, no una región inventada: es preferible
    un hueco visible a un dato falso que nadie detecta.
    """
    column = F.col(state_column)
    expression = None

    for region, states in BRAZIL_REGIONS.items():
        condition = column.isin(states)
        expression = (
            F.when(condition, F.lit(region))
            if expression is None
            else expression.when(condition, F.lit(region))
        )

    return expression.otherwise(F.lit(None))


def build_dim_geography(spark: SparkSession) -> DataFrame:
    """Construye la dimensión de geografía."""
    geolocation = read_silver(spark, "geolocation")

    dim_geography = geolocation.select(
        F.sha2(F.col("zip_code_prefix"), 256).alias("geography_sk"),
        F.col("zip_code_prefix"),
        F.col("city"),
        F.col("state"),
        region_column("state").alias("region"),
        F.col("latitude"),
        F.col("longitude"),
    )

    return with_unknown_member(dim_geography, surrogate_key="geography_sk")
