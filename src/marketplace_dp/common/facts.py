"""Utilidades comunes a la construcción de tablas de hechos en la capa Gold.

Construir un hecho es, sobre todo, **traducir claves naturales a claves
sustitutas**. Estas funciones concentran las dos reglas que toda tabla de hechos
debe cumplir para no perder filas por el camino.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Miembro desconocido. Debe coincidir con el que usan las dimensiones.
UNKNOWN_KEY = "-1"
UNKNOWN_DATE_KEY = -1


def date_key(column: str) -> Column:
    """Convierte un timestamp en la clave sustituta de `dim_date` (yyyyMMdd).

    Debe generar exactamente la misma expresión que `dim_date`, o las claves no
    casarían y todos los joins de fecha fallarían.

    Una fecha ausente —una orden que nunca se entregó— se resuelve al miembro
    desconocido en lugar de quedar en NULL: con NULL, la fila entera desaparece
    al unirse con `dim_date`.
    """
    return F.coalesce(
        F.date_format(F.col(column), "yyyyMMdd").cast("int"),
        F.lit(UNKNOWN_DATE_KEY),
    )


def resolve_key(column: str) -> Column:
    """Resuelve una clave sustituta ausente al miembro desconocido.

    Se aplica SIEMPRE después de un LEFT JOIN contra una dimensión, aunque hoy no
    haya nulos: es una red de seguridad, no una corrección. El día que llegue un
    hecho cuya entidad no exista en la dimensión, la fila debe seguir siendo
    visible como "Unknown" en lugar de evaporarse en el siguiente join.
    """
    return F.coalesce(F.col(column), F.lit(UNKNOWN_KEY))
