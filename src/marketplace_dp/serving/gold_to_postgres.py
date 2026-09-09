"""Materializa las tablas de Gold desde el lakehouse hacia PostgreSQL.

Un lakehouse es eficiente escaneando millones de filas, pero pobre en consultas
pequeñas y concurrentes — que es exactamente el patrón de Power BI, de una API y
de un analista explorando. PostgreSQL da SQL estándar, conectores maduros y baja
latencia por consulta, y las tablas de Gold son agregados que caben holgadamente.

Esta materialización es también el punto de entrada de dbt: los modelos leerán
estas tablas como `sources` y construirán encima los *marts* de negocio.

    python -m marketplace_dp.serving.gold_to_postgres
    python -m marketplace_dp.serving.gold_to_postgres --table dim_date fct_orders
"""

from __future__ import annotations

import argparse
import time
from uuid import uuid4

from pyspark.sql import SparkSession
from structlog.contextvars import bind_contextvars, unbind_contextvars

from marketplace_dp.common.config import settings
from marketplace_dp.common.lakehouse import read_gold
from marketplace_dp.common.logging import configure_logging, logger
from marketplace_dp.common.spark import get_spark

# Esquema de destino en PostgreSQL. Se mantiene separado de `public` para que dbt
# construya sus modelos en otro esquema y quede claro qué es origen y qué es
# transformación.
TARGET_SCHEMA = "gold"

GOLD_TABLES = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_geography",
    "dim_seller",
    "fct_orders",
    "fct_order_items",
    "fct_payments",
    "fct_reviews",
]


def jdbc_url() -> str:
    return f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"


def jdbc_properties() -> dict[str, str]:
    return {
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def load_table(spark: SparkSession, table: str) -> int:
    """Copia una tabla de Gold al warehouse. Devuelve las filas escritas."""
    start = time.perf_counter()
    bind_contextvars(table=table)

    try:
        df = read_gold(spark, table)
        rows = df.count()

        (
            df.write.mode("overwrite")
            # `truncate` reutiliza la tabla en vez de borrarla y recrearla, así se
            # conservan los permisos y las vistas que dependan de ella.
            .option("truncate", "true")
            # Sin esto Spark abriría una conexión por partición; con 8 particiones
            # y 9 tablas serían 72 conexiones simultáneas contra PostgreSQL.
            .option("batchsize", 10_000)
            .jdbc(jdbc_url(), f"{TARGET_SCHEMA}.{table}", properties=jdbc_properties())
        )

        logger.info(
            "postgres_loaded",
            rows=rows,
            target=f"{TARGET_SCHEMA}.{table}",
            duration_seconds=time.perf_counter() - start,
        )
        return rows

    except Exception:
        logger.exception("postgres_load_failed", target=f"{TARGET_SCHEMA}.{table}")
        raise

    finally:
        unbind_contextvars("table")


def ensure_schema() -> None:
    """Crea el esquema de destino si no existe.

    Spark no puede ejecutar DDL arbitrario por JDBC, así que se usa psycopg2, que
    viene con dbt-postgres.
    """
    import psycopg2

    with psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
        connection.commit()

    logger.info("schema_ready", schema=TARGET_SCHEMA)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materializa Gold en PostgreSQL")
    parser.add_argument(
        "--table",
        nargs="+",
        default=None,
        help="Tablas a materializar. Por defecto, todas.",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()

    tables = args.table or GOLD_TABLES
    unknown = set(tables) - set(GOLD_TABLES)
    if unknown:
        raise ValueError(f"Tablas desconocidas: {sorted(unknown)}. Disponibles: {GOLD_TABLES}")

    bind_contextvars(run_id=str(uuid4()))
    logger.info("gold_to_postgres_started", tables=tables)
    start = time.perf_counter()

    spark = get_spark("gold-to-postgres")
    results: dict[str, int] = {}
    try:
        ensure_schema()
        for table in tables:
            results[table] = load_table(spark, table)

        logger.info(
            "gold_to_postgres_finished",
            tables=len(results),
            total_rows=sum(results.values()),
            rows_by_table=results,
            duration_seconds=time.perf_counter() - start,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
