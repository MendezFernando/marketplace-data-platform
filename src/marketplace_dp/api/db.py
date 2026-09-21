"""Conexión de la API con PostgreSQL.

En lugar de abrir una conexión nueva por cada petición (lento), se mantienen
unas cuantas abiertas y se van prestando. Es como una flotilla de taxis.
"""

from __future__ import annotations

from collections.abc import Iterator

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from marketplace_dp.common.config import settings

# Esquema donde dbt publica los marts.
MARTS_SCHEMA = "dbt_marts"

pool = ConnectionPool(
    (
        f"host={settings.postgres_host} port={settings.postgres_port} "
        f"dbname={settings.postgres_db} user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    ),
    min_size=1,  # siempre al menos una conexión lista
    max_size=5,  # nunca más de cinco, aunque lleguen muchas peticiones
    open=False,  # se abre cuando arranca la API, no al importar este archivo
    kwargs={"row_factory": dict_row},  # cada fila llega como diccionario
)


def get_connection() -> Iterator[Connection]:
    """Presta una conexión mientras dura la petición y luego la devuelve."""
    with pool.connection() as conn:
        yield conn
