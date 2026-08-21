from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from structlog.contextvars import bind_contextvars, unbind_contextvars

from marketplace_dp.common.lakehouse import publish_gold
from marketplace_dp.common.logging import configure_logging, logger
from marketplace_dp.common.spark import get_spark
from marketplace_dp.gold_transformation.dim_customer import build_dim_customer
from marketplace_dp.gold_transformation.dim_date import build_dim_date
from marketplace_dp.gold_transformation.dim_geography import build_dim_geography
from marketplace_dp.gold_transformation.dim_product import build_dim_product


@dataclass(frozen=True)
class Entity:
    """Declaración de una entidad de Gold."""

    name: str
    primary_key: list[str]
    builder: Callable[[SparkSession], DataFrame]


# El orden importa: las entidades de referencia se construyen antes que los
# hechos que las apuntan, para que un fallo temprano no deje Silver a medias.
ENTITIES: list[Entity] = [
    Entity("dim_date", ["date_sk"], build_dim_date),
    Entity("dim_customer", ["customer_sk"], build_dim_customer),
    Entity("dim_product", ["product_sk"], build_dim_product),
    Entity("dim_geography", ["geography_sk"], build_dim_geography),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye la capa Gold desde Silver")
    parser.add_argument(
        "--entity",
        nargs="+",
        default=None,
        help="Entidades a construir. Por defecto, todas.",
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


def build_entity(spark: SparkSession, entity: Entity) -> int:
    """Construye y publica una entidad. Devuelve las filas escritas."""
    start = time.perf_counter()

    bind_contextvars(table=entity.name)

    try:
        logger.info("entity_started")
        rows = publish_gold(entity.builder(spark), entity.name, entity.primary_key)
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
        unbind_contextvars("table")


def main() -> None:
    configure_logging()
    args = parse_args()

    bind_contextvars(run_id=str(uuid4()))
    entities = select_entities(args.entity)

    logger.info("gold_run_started", entities=[e.name for e in entities])
    start = time.perf_counter()

    spark = get_spark("silver-to-gold")
    results: dict[str, int] = {}

    try:
        for entity in entities:
            # Fail-fast deliberado: las entidades de Silver se consumen juntas y
            # publicar un subconjunto dejaría la capa en estado incoherente.
            # El proceso termina con código distinto de cero para que el
            # orquestador decida si reintenta.
            results[entity.name] = build_entity(spark, entity)

        logger.info(
            "gold_run_finished",
            entities=len(results),
            total_rows=sum(results.values()),
            rows_by_entity=results,
            duration_seconds=time.perf_counter() - start,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
