"""Utilidades comunes a la construcción de dimensiones en la capa Gold."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

UNKNOWN_KEY = "-1"
UNKNOWN_LABEL = "Unknown"


def with_unknown_member(
    df: DataFrame,
    surrogate_key: str,
    unknown_key: str = UNKNOWN_KEY,
    unknown_label: str = UNKNOWN_LABEL,
) -> DataFrame:
    """Añade a la dimensión la fila del *miembro desconocido*.

    Un hecho cuya clave foránea sea NULL **desaparece** al unirse con la
    dimensión: un JOIN descarta los NULL en silencio y la fila deja de aparecer
    en el reporte sin que nadie lo note. Apuntando en cambio a esta fila
    especial, el hecho sobrevive y el reporte muestra explícitamente
    "Unknown", que es información y no ausencia.

    La fila se construye **derivando el esquema del propio DataFrame**, de modo
    que no hay que declarar un `StructType` por dimensión ni mantenerlo
    sincronizado cuando se añada una columna:

    - la clave sustituta recibe `unknown_key`, casteado al tipo de la columna
      (entero para `dim_date`, texto para las dimensiones con clave de hash);
    - las columnas de texto reciben `unknown_label`, para que el reporte muestre
      una etiqueta legible en lugar de un hueco;
    - el resto (números, fechas, booleanos) reciben NULL, porque inventar un 0 o
      un `false` sería afirmar algo falso sobre el dato.
    """
    if surrogate_key not in df.columns:
        raise ValueError(
            f"La clave sustituta '{surrogate_key}' no existe en el DataFrame. "
            f"Columnas disponibles: {df.columns}"
        )

    values = []
    for field in df.schema.fields:
        if field.name == surrogate_key:
            value = F.lit(unknown_key).cast(field.dataType)
        elif isinstance(field.dataType, StringType):
            value = F.lit(unknown_label).cast(field.dataType)
        else:
            value = F.lit(None).cast(field.dataType)
        values.append(value.alias(field.name))

    # range(1) produce exactamente una fila sobre la que proyectar los literales;
    # es la forma idiomática de crear un DataFrame desde cero en Spark.
    unknown = df.sparkSession.range(1).select(*values)

    # unionByName, no union: une por NOMBRE de columna y no por posición, así que
    # sigue siendo correcto aunque el orden de las columnas cambie en el futuro.
    return unknown.unionByName(df)
