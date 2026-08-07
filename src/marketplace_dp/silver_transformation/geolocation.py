import time
from datetime import date
from uuid import uuid4

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from structlog.contextvars import bind_contextvars

from marketplace_dp.common.config import settings
from marketplace_dp.common.logging import configure_logging, logger
from marketplace_dp.common.partitions import resolve_ingestion_date
from marketplace_dp.common.spark import get_spark


def build_geolocation(spark: SparkSession, name: str, ingestion_date: date) -> None:
    """Lee la tabla geolocation de Bronze, la transforma y la escribe en Silver."""

    start = time.perf_counter()

    bronze_path = None
    silver_path = None
    silver_geo = None
    validation = None

    try:
        logger.info("build_geolocation_started", table=name)

        # ─── Lee Bronze ────────────────────────────────────────────────

        bronze_path = (
            f"s3a://{settings.minio_bucket_bronze}/"
            f"olist/olist_geolocation_dataset/"
            f"ingestion_date={ingestion_date}/"
            f"olist_geolocation_dataset.parquet"
        )

        geo = spark.read.parquet(bronze_path)

        rows_bronze = geo.count()

        logger.info("bronze_loaded", rows=rows_bronze, path=bronze_path)

        # ─── Tipado ───────────────────────────────────────────────────

        tipado = (
            geo.withColumn(
                "geolocation_zip_code_prefix", geo["geolocation_zip_code_prefix"].cast("string")
            )
            .withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
            .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))
            .withColumn("geolocation_city", F.col("geolocation_city").cast("string"))
            .withColumn("geolocation_state", F.col("geolocation_state").cast("string"))
        )

        # ─── Coordenadas medianas por ZIP ─────────────────────────────

        mediana_coordenadas = tipado.groupBy("geolocation_zip_code_prefix").agg(
            F.percentile_approx("geolocation_lat", 0.5).alias("lat_mediana"),
            F.percentile_approx("geolocation_lng", 0.5).alias("lng_mediana"),
        )

        # ─── Ciudad y estado más frecuentes ───────────────────────────

        city_state_count = tipado.groupBy(
            "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"
        ).agg(F.count("*").alias("n"))

        window = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
            F.desc("n"), F.asc("geolocation_city")
        )

        geo_moda = (
            city_state_count.withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .select(
                "geolocation_zip_code_prefix",
                F.col("geolocation_city").alias("city"),
                F.col("geolocation_state").alias("state"),
            )
        )

        # ─── Construcción Silver ──────────────────────────────────────

        silver_geo = mediana_coordenadas.join(
            geo_moda, "geolocation_zip_code_prefix", "left"
        ).cache()

        # ─── Validación PK ────────────────────────────────────────────

        validation = silver_geo.agg(
            F.count("*").alias("rows"),
            F.countDistinct("geolocation_zip_code_prefix").alias("distinct_pk"),
        ).first()

        if validation["distinct_pk"] != validation["rows"]:
            raise ValueError(
                "Violación de clave primaria: "
                f"{validation['distinct_pk']} distintos "
                f"vs {validation['rows']} filas"
            )

        # ─── Escritura Delta ──────────────────────────────────────────

        silver_path = f"s3a://{settings.minio_bucket_silver}/{name}"

        (silver_geo.write.format("delta").mode("overwrite").save(silver_path))

        logger.info("silver_geo_written", silver_path=silver_path, rows_silver=validation["rows"])

        logger.info(
            "build_geolocation_finished",
            rows=validation["rows"],
            duration_seconds=time.perf_counter() - start,
        )

    except ValueError:
        logger.exception("primary_key_validation_failed", table=name)
        raise

    except Exception:
        logger.exception("bronze_to_silver_failed", table=name, path=bronze_path)
        raise

    finally:
        if silver_geo is not None:
            silver_geo.unpersist()


def main():

    configure_logging()

    ingestion_date = resolve_ingestion_date("olist_geolocation_dataset", None)

    bind_contextvars(run_id=str(uuid4()), ingestion_date=ingestion_date.isoformat())

    spark = get_spark("bronze-to-silver")

    build_geolocation(spark, "geolocation", ingestion_date)


if __name__ == "__main__":
    main()
