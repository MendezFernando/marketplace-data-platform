"""Orquestador de la capa Silver: construye todas las entidades en una ejecución.

Una única SparkSession para todas las entidades. Arrancar la JVM cuesta entre 10
y 20 segundos: ocho procesos independientes serían dos minutos de puro arranque.

    python -m marketplace_dp.silver_transformation.run_all
    python -m marketplace_dp.silver_transformation.run_all --entity orders payments
    python -m marketplace_dp.silver_transformation.run_all --ingestion-date 2026-08-01

────────────────────────────────────────────────────────────────────────────────
 PARA AÑADIR LAS ENTIDADES QUE FALTAN (customers, geolocation, products, sellers)
────────────────────────────────────────────────────────────────────────────────
 Cada módulo existente hace hoy tres cosas: transformar, validar y escribir.
 Para integrarlo aquí basta con dejarle solo la primera:

   1. Cambia la firma a  `build_x(spark, ingestion_date) -> DataFrame`
   2. Borra la validación de PK, la escritura, el logging y el try/except:
      todo eso vive ahora en `publish_silver()`
   3. Sustituye la lectura manual por `read_bronze(spark, tabla, ingestion_date)`
      y registra la entidad en ENTITIES

 Cada módulo debería quedarse en 20-30 líneas de lógica de negocio pura.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from structlog.contextvars import bind_contextvars, unbind_contextvars

from marketplace_dp.common.lakehouse import publish_silver
from marketplace_dp.common.logging import configure_logging, logger
from marketplace_dp.common.partitions import resolve_ingestion_date
from marketplace_dp.common.spark import get_spark
from marketplace_dp.silver_transformation.customers import build_customers
from marketplace_dp.silver_transformation.geolocation import build_geolocation
from marketplace_dp.silver_transformation.order_items import build_order_items
from marketplace_dp.silver_transformation.orders import build_orders
from marketplace_dp.silver_transformation.payments import build_payments
from marketplace_dp.silver_transformation.products import build_products
from marketplace_dp.silver_transformation.reviews import build_reviews
from marketplace_dp.silver_transformation.sellers import build_sellers


@dataclass(frozen=True)
class Entity:
    """Declaración de una entidad de Silver."""

    name: str
    source_table: str  # tabla de Bronze usada para resolver la partición
    primary_key: list[str]
    builder: Callable[[SparkSession, date], DataFrame]


# El orden importa: las entidades de referencia se construyen antes que los
# hechos que las apuntan, para que un fallo temprano no deje Silver a medias.
ENTITIES: list[Entity] = [
    Entity("customers", "olist_customers_dataset", ["customer_unique_id"], build_customers),
    Entity("sellers", "olist_sellers_dataset", ["seller_id"], build_sellers),
    Entity("products", "olist_products_dataset", ["product_id"], build_products),
    Entity("geolocation", "olist_geolocation_dataset", ["zip_code_prefix"], build_geolocation),
    Entity("orders", "olist_orders_dataset", ["order_id"], build_orders),
    Entity(
        "order_items",
        "olist_order_items_dataset",
        ["order_id", "order_item_id"],
        build_order_items,
    ),
    Entity(
        "payments",
        "olist_order_payments_dataset",
        ["order_id", "payment_sequential"],
        build_payments,
    ),
    Entity("reviews", "olist_order_reviews_dataset", ["review_id", "order_id"], build_reviews),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye la capa Silver desde Bronze")
    parser.add_argument(
        "--entity",
        nargs="+",
        default=None,
        help="Entidades a construir. Por defecto, todas.",
    )
    parser.add_argument(
        "--ingestion-date",
        default=None,
        help="Partición de Bronze a leer (YYYY-MM-DD). Por defecto, la más reciente.",
    )
    return parser.parse_args()


def select_entities(requested: list[str] | None) -> list[Entity]:
    if not requested:
        return ENTITIES

    known = {e.name: e for e in ENTITIES}
    unknown = set(requested) - known.keys()
    if unknown:
        raise ValueError(f"Entidades desconocidas: {sorted(unknown)}. Disponibles: {sorted(known)}")

    return [known[n] for n in requested]


def build_entity(spark: SparkSession, entity: Entity, requested_date: str | None) -> int:
    """Construye y publica una entidad. Devuelve las filas escritas."""
    start = time.perf_counter()

    ingestion_date = resolve_ingestion_date(entity.source_table, requested_date)
    bind_contextvars(table=entity.name, ingestion_date=ingestion_date.isoformat())

    try:
        logger.info("entity_started")
        rows = publish_silver(
            entity.builder(spark, ingestion_date), entity.name, entity.primary_key
        )
        logger.info("entity_finished", rows=rows, duration_seconds=time.perf_counter() - start)
        return rows

    except ValueError:
        # Fallo de calidad de datos: el dato es incorrecto, no la infraestructura.
        logger.exception("entity_data_quality_failed", primary_key=entity.primary_key)
        raise

    except Exception:
        # Fallo técnico: lectura, escritura o conectividad.
        logger.exception("entity_technical_failed")
        raise

    finally:
        unbind_contextvars("table", "ingestion_date")


def main() -> None:
    configure_logging()
    args = parse_args()

    bind_contextvars(run_id=str(uuid4()))
    entities = select_entities(args.entity)

    logger.info("silver_run_started", entities=[e.name for e in entities])
    start = time.perf_counter()

    spark = get_spark("bronze-to-silver")
    results: dict[str, int] = {}

    try:
        for entity in entities:
            # Fail-fast deliberado: las entidades de Silver se consumen juntas y
            # publicar un subconjunto dejaría la capa en estado incoherente.
            # El proceso termina con código distinto de cero para que el
            # orquestador decida si reintenta.
            results[entity.name] = build_entity(spark, entity, args.ingestion_date)

        logger.info(
            "silver_run_finished",
            entities=len(results),
            total_rows=sum(results.values()),
            rows_by_entity=results,
            duration_seconds=time.perf_counter() - start,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
