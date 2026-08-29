"""Gold — `dim_seller`: dimensión de vendedores con SCD Tipo 2.

Es la única dimensión que conserva historial. BQ-04 exige atribuir cada venta al
segmento que el vendedor tenía **en la fecha de compra**: si vendió como Bronze en
enero y hoy es Gold, esa venta debe seguir contando como Bronze. Sin historial, el
mismo informe daría un número distinto cada mes.

Se carga con `MERGE` de Delta Lake en lugar de `overwrite`, porque las versiones
anteriores deben preservarse.
"""

from __future__ import annotations

import argparse
import time
from datetime import date, timedelta
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from structlog.contextvars import bind_contextvars

from marketplace_dp.common.config import settings
from marketplace_dp.common.lakehouse import read_silver
from marketplace_dp.common.logging import configure_logging, logger
from marketplace_dp.common.spark import get_spark

# ─────────────────────────────────────────────────────────────────────────────
#  Reglas de negocio — decisión abierta en docs/02-gold-dimensional-model.md §8.
#
#  Con ventana de 90 días y estos umbrales NINGÚN vendedor alcanza Gold
#  (Gold=0, Silver=106, Bronze=2989). Con 365 días la distribución es
#  Gold=27, Silver=422, Bronze=2646. Pendiente de acordar con negocio.
# ─────────────────────────────────────────────────────────────────────────────
SEGMENT_WINDOW_DAYS = 365
SILVER_THRESHOLD = 5_000
GOLD_THRESHOLD = 50_000

# Fecha "infinito": permite escribir `fecha BETWEEN valid_from AND valid_to` sin
# tratar el NULL como caso especial en cada consulta.
FUTURE_DATE = "9999-12-31"

# Fecha "principio de los tiempos", SOLO para la carga inicial.
#
# La primera carga afirma "esto es lo que sé de estas entidades, y hasta donde sé
# siempre fue así". Si la vigencia empezara en la fecha de carga, todos los hechos
# ANTERIORES a esa fecha quedarían huérfanos: no encajarían en ninguna ventana y
# caerían al miembro desconocido. Con datos reales eso supuso el 45% del GMV sin
# atribuir.
INITIAL_VALID_FROM = "1900-01-01"

# Atributos cuyo cambio abre una versión nueva. El GMV queda fuera a propósito:
# si entrara aquí, cada céntimo de variación crearía una fila y la dimensión
# crecería sin control.
TRACKED_ATTRIBUTES = [
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
    "seller_segment",
]

BUSINESS_COLUMNS = ["seller_id", *TRACKED_ATTRIBUTES, "gmv_window"]
SCD_COLUMNS = ["valid_from", "valid_to", "is_current"]
DIMENSION_COLUMNS = ["seller_sk", *BUSINESS_COLUMNS, *SCD_COLUMNS]

UNKNOWN_KEY = "-1"
UNKNOWN_LABEL = "Unknown"


def _segment_expression(gmv: Column) -> Column:
    """Traduce el GMV de la ventana al segmento comercial."""
    return (
        F.when(gmv > GOLD_THRESHOLD, F.lit("Gold"))
        .when(gmv >= SILVER_THRESHOLD, F.lit("Silver"))
        .otherwise(F.lit("Bronze"))
    )


def _surrogate_key(seller_id: Column, valid_from: Column) -> Column:
    """Clave sustituta = hash(seller_id | valid_from).

    `valid_from` forma parte del hash porque un vendedor tiene VARIAS filas: sin
    la fecha, sus tres versiones compartirían clave y volveríamos al problema que
    SCD2 resuelve.

    `concat_ws` con separador '|' evita colisiones: sin él, ("ab","1231") y
    ("ab1","231") producirían la misma cadena y por tanto el mismo hash.
    """
    return F.sha2(F.concat_ws("|", seller_id, valid_from.cast("string")), 256)


def seller_snapshot(spark: SparkSession, as_of: date) -> DataFrame:
    """Estado de todos los vendedores a una fecha dada: atributos + segmento.

    Es una foto, sin noción de historial. El versionado lo aplica el MERGE.
    """
    sellers = read_silver(spark, "sellers")
    items = read_silver(spark, "order_items")
    orders = read_silver(spark, "orders").select("order_id", "order_purchase_timestamp")

    window_start = as_of - timedelta(days=SEGMENT_WINDOW_DAYS)

    gmv = (
        items.join(orders, on="order_id", how="inner")
        .filter(F.col("order_purchase_timestamp").between(F.lit(window_start), F.lit(as_of)))
        .groupBy("seller_id")
        .agg(F.sum("price").alias("gmv_window"))
    )

    # LEFT JOIN: un vendedor sin ventas en la ventana debe seguir existiendo en la
    # dimensión (GMV 0 -> Bronze). Con INNER desaparecería de la dimensión.
    snapshot = (
        sellers.join(gmv, on="seller_id", how="left")
        .withColumn("gmv_window", F.coalesce(F.col("gmv_window"), F.lit(0).cast("decimal(12,2)")))
        .withColumn("seller_segment", _segment_expression(F.col("gmv_window")))
        .select(*BUSINESS_COLUMNS)
    )

    # Miembro desconocido: un hecho que apunte a un vendedor inexistente debe
    # sobrevivir al JOIN en lugar de desaparecer en silencio.
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_KEY).alias("seller_id"),
        *[F.lit(UNKNOWN_LABEL).alias(c) for c in TRACKED_ATTRIBUTES],
        F.lit(0).cast("decimal(12,2)").alias("gmv_window"),
    )

    return unknown.unionByName(snapshot)


def _as_new_version(snapshot: DataFrame, as_of: date) -> DataFrame:
    """Convierte la foto en filas de dimensión: clave sustituta y vigencia."""
    return (
        snapshot.withColumn("valid_from", F.lit(as_of).cast("date"))
        .withColumn("valid_to", F.lit(FUTURE_DATE).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "seller_sk",
            F.when(F.col("seller_id") == UNKNOWN_KEY, F.lit(UNKNOWN_KEY)).otherwise(
                _surrogate_key(F.col("seller_id"), F.col("valid_from"))
            ),
        )
        .select(*DIMENSION_COLUMNS)
    )


def _change_condition() -> str:
    """Expresión SQL que detecta si algún atributo versionado cambió.

    Usa `<=>` (igualdad *null-safe* de Spark) en vez de `<>`: en SQL estándar
    `NULL <> 'x'` devuelve NULL, que se comporta como falso, así que un atributo
    que pasa de NULL a un valor NO se detectaría como cambio. Es un fallo
    silencioso clásico en implementaciones de SCD2.
    """
    return " OR ".join(f"NOT (target.{c} <=> source.{c})" for c in TRACKED_ATTRIBUTES)


def load_dim_seller(spark: SparkSession, as_of: date) -> dict[str, int]:
    """Carga `dim_seller` aplicando SCD Tipo 2. Devuelve métricas de la operación."""
    path = f"s3a://{settings.minio_bucket_gold}/dim_seller"
    snapshot = seller_snapshot(spark, as_of)

    # ── Carga inicial ────────────────────────────────────────────────────────
    # DeltaTable.forPath falla si la tabla no existe: la primera ejecución no
    # puede hacer MERGE contra la nada.
    if not DeltaTable.isDeltaTable(spark, path):
        # La carga inicial arranca en el "principio de los tiempos" para que los
        # hechos históricos encuentren su versión. Las cargas posteriores sí usan
        # `as_of`, porque ahí sí sabemos cuándo ocurrió el cambio.
        initial = _as_new_version(snapshot, date.fromisoformat(INITIAL_VALID_FROM))
        rows = initial.count()
        initial.write.format("delta").mode("overwrite").save(path)
        logger.info("scd2_initial_load", table="dim_seller", path=path, rows=rows)
        return {"inserted": rows, "closed": 0, "changed_detected": 0}

    # ── Carga incremental con MERGE ──────────────────────────────────────────
    target = DeltaTable.forPath(spark, path)
    current = target.toDF().filter(F.col("is_current")).alias("cur")
    candidate = _as_new_version(snapshot, as_of).alias("new")

    # Qué vendedores cambiaron: comparación null-safe contra la versión vigente.
    differs = None
    for column in TRACKED_ATTRIBUTES:
        expression = ~F.col(f"new.{column}").eqNullSafe(F.col(f"cur.{column}"))
        differs = expression if differs is None else (differs | expression)

    changed = (
        candidate.join(current, F.col("new.seller_id") == F.col("cur.seller_id"), "inner")
        .filter(differs)
        .select("new.*")
    )
    changed_count = changed.count()

    # ── El truco del doble paso ──────────────────────────────────────────────
    # Un MERGE dispara UNA acción por fila emparejada, pero un cambio de atributo
    # necesita DOS: cerrar la versión vigente (UPDATE) y abrir la nueva (INSERT).
    # Se resuelve emitiendo dos filas para cada vendedor que cambió:
    #   · merge_key = seller_id -> empareja      -> UPDATE (cierra la vieja)
    #   · merge_key = NULL      -> nunca empareja -> INSERT (abre la nueva)
    to_close = candidate.withColumn("merge_key", F.col("seller_id"))
    to_open = changed.withColumn("merge_key", F.lit(None).cast("string"))
    source = to_close.unionByName(to_open)

    (
        target.alias("target")
        .merge(
            source.alias("source"),
            # `is_current = true` es imprescindible: sin él compararíamos contra
            # versiones ya cerradas y volveríamos a cerrarlas.
            "target.seller_id = source.merge_key AND target.is_current = true",
        )
        .whenMatchedUpdate(
            condition=_change_condition(),
            set={
                "valid_to": "date_sub(source.valid_from, 1)",
                "is_current": "false",
            },
        )
        .whenNotMatchedInsert(values={c: f"source.{c}" for c in DIMENSION_COLUMNS})
        .execute()
    )

    metrics = (
        spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1")
        .select("operationMetrics")
        .first()["operationMetrics"]
    )
    result = {
        "closed": int(metrics.get("numTargetRowsUpdated", 0)),
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "changed_detected": changed_count,
    }
    logger.info("scd2_merge_finished", table="dim_seller", path=path, **result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye dim_seller con SCD Tipo 2")
    parser.add_argument(
        "--as-of",
        default=None,
        help="Fecha de referencia (YYYY-MM-DD). Determina la ventana de GMV y la "
        "vigencia de las versiones nuevas. Por defecto, hoy.",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()

    bind_contextvars(run_id=str(uuid4()), as_of=as_of.isoformat())
    logger.info("dim_seller_started", window_days=SEGMENT_WINDOW_DAYS)

    start = time.perf_counter()
    spark = get_spark("dim-seller")
    try:
        result = load_dim_seller(spark, as_of)
        logger.info("dim_seller_finished", duration_seconds=time.perf_counter() - start, **result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
