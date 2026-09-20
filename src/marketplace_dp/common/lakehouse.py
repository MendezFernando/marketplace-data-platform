"""Acceso a las tablas del lakehouse: lectura de Bronze y Silver, escritura en Delta.

Concentra el andamiaje que repiten todas las transformaciones —leer una partición
o una tabla, validar la clave primaria, escribir Delta y registrar el resultado—
para que cada módulo de entidad contenga únicamente su lógica de negocio.

Es transversal a las capas: lo usan tanto `silver_transformation` como
`gold_transformation`, porque el mecanismo de publicación es idéntico y solo
cambia el bucket de destino.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from marketplace_dp.common.config import settings
from marketplace_dp.common.logging import logger


def bronze_path(table: str, ingestion_date: date) -> str:
    """Construye la ruta de una partición concreta de Bronze."""
    return (
        f"s3a://{settings.bucket_bronze}/olist/{table}/"
        f"ingestion_date={ingestion_date.isoformat()}/"
    )


def read_bronze(spark: SparkSession, table: str, ingestion_date: date) -> DataFrame:
    """Lee UNA partición de Bronze y registra cuántas filas trajo.

    Se lee siempre una única partición: leer la carpeta completa devolvería la
    unión de todos los snapshots diarios y reintroduciría registros eliminados
    en el origen.
    """
    path = bronze_path(table, ingestion_date)
    df = spark.read.parquet(path)

    logger.info("bronze_loaded", table=table, path=path, rows=df.count())
    return df


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    """Lee una tabla de Silver y registra cuántas filas trajo."""
    path = f"s3a://{settings.bucket_silver}/{table}"
    df = spark.read.format("delta").load(path)

    logger.info("silver_loaded", table=table, path=path, rows=df.count())
    return df


def read_gold(spark: SparkSession, table: str) -> DataFrame:
    """Lee una tabla de Gold y registra cuántas filas trajo."""
    path = f"s3a://{settings.bucket_gold}/{table}"
    df = spark.read.format("delta").load(path)

    logger.info("gold_loaded", table=table, path=path, rows=df.count())
    return df


def publish_entity(
    df: DataFrame,
    name: str,
    primary_key: list[str],
    bucket: str,
    overwrite_schema: bool = False,
) -> int:
    """Valida la clave primaria y publica una entidad como tabla Delta.

    Devuelve el número de filas escritas. La usan tanto Silver como Gold: la
    validación, el cacheo y el registro son idénticos; solo cambia el bucket.

    El DataFrame se cachea porque se recorre dos veces (validación y escritura);
    sin caché, Spark recalcularía el plan completo en cada acción.

    `overwrite_schema` desactiva el *schema enforcement* de Delta y por defecto
    está apagado: si el esquema cambia, la escritura DEBE fallar. Renombrar o
    eliminar una columna rompe a los consumidores aguas abajo, así que es una
    migración deliberada, no un efecto colateral de un despliegue.
    """
    cached = df.cache()

    try:
        validation = cached.agg(
            F.count("*").alias("rows"),
            F.countDistinct(*[F.col(c) for c in primary_key]).alias("distinct_pk"),
        ).first()

        rows = validation["rows"]
        distinct_pk = validation["distinct_pk"]

        if distinct_pk != rows:
            raise ValueError(
                f"Violación de clave primaria en '{name}' por {primary_key}: "
                f"{distinct_pk} combinaciones distintas frente a {rows} filas"
            )

        path = f"s3a://{bucket}/{name}"
        writer = cached.write.format("delta").mode("overwrite")
        if overwrite_schema:
            logger.warning("schema_overwrite", table=name, columns=cached.columns)
            writer = writer.option("overwriteSchema", "true")
        writer.save(path)

        logger.info(
            "entity_written",
            table=name,
            path=path,
            rows=rows,
            primary_key=primary_key,
            columns=len(cached.columns),
        )
        return rows

    finally:
        cached.unpersist()


def publish_silver(
    df: DataFrame, name: str, primary_key: list[str], overwrite_schema: bool = False
) -> int:
    """Publica una entidad en la capa Silver."""
    return publish_entity(df, name, primary_key, settings.bucket_silver, overwrite_schema)


def publish_gold(
    df: DataFrame, name: str, primary_key: list[str], overwrite_schema: bool = False
) -> int:
    """Publica una dimensión o tabla de hechos en la capa Gold."""
    return publish_entity(df, name, primary_key, settings.bucket_gold, overwrite_schema)


def clean_text(column: str) -> F.Column:
    """Normaliza texto libre: recorta espacios, colapsa los internos y baja a minúsculas.

    Las cadenas vacías se convierten en NULL: en Bronze se conservó el texto tal
    cual llegó (`keep_default_na=False`), y es en Silver donde "sin valor" pasa a
    representarse como ausencia real.
    """
    normalized = F.lower(F.trim(F.regexp_replace(F.col(column), r"\s+", " ")))
    return F.when(normalized == "", None).otherwise(normalized)


def clean_code(column: str) -> F.Column:
    """Normaliza códigos cortos (estados, siglas): recorta y pasa a mayúsculas."""
    normalized = F.upper(F.trim(F.col(column)))
    return F.when(normalized == "", None).otherwise(normalized)


def to_timestamp(column: str) -> F.Column:
    """Convierte texto a timestamp con el formato del origen.

    Los valores no parseables se convierten en NULL en lugar de fallar: en Silver
    una fecha ausente es un hecho de negocio legítimo (una orden no entregada no
    tiene fecha de entrega), no un error del pipeline.
    """
    return F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")


def to_money(column: str) -> F.Column:
    """Convierte importes monetarios a DECIMAL, nunca a FLOAT.

    FLOAT es binario y no representa exactamente los decimales: acumulado sobre
    millones de filas produce descuadres de céntimos.
    """
    return F.col(column).cast("decimal(12, 2)")
