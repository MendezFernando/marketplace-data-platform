from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_DNS, uuid4, uuid5

import pandas as pd
from structlog.contextvars import bind_contextvars

from marketplace_dp.common.config import settings
from marketplace_dp.common.logging import configure_logging, logger


def parse_args():
    """
    Lee argumentos desde la terminal.
    """

    parser = argparse.ArgumentParser(description="Ingest CSV files into Bronze layer")

    parser.add_argument("--table", required=True, nargs="+", help="Nombre de la tabla o 'all'")

    return parser.parse_args()


def get_tables(table_names: list[str]) -> list[str]:
    """
    Obtiene las tablas que se van a procesar.
    """

    landing = Path(settings.landing_path)

    if "all" in table_names:
        return [file.stem for file in landing.glob("*.csv")]

    return table_names


def read_csv(table: str) -> tuple[pd.DataFrame, Path]:
    """
    Lee un CSV manteniendo todos los valores como texto.
    """

    file_path = Path(settings.landing_path) / f"{table}.csv"

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo CSV: {file_path}")

    logger.info("reading_csv", file=str(file_path))

    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

    return df, file_path


def add_metadata(
    df: pd.DataFrame,
    source_file: Path,
    ingested_at: datetime,
    batch_id: str,
) -> pd.DataFrame:
    """
    Agrega columnas de auditoría.
    """

    new_df = df.copy()

    new_df["_ingested_at"] = ingested_at
    new_df["_source_file"] = source_file.name
    new_df["_batch_id"] = batch_id

    return new_df


def write_bronze(
    df: pd.DataFrame,
    ingested_at: datetime,
    table: str,
):
    """
    Escribe Parquet en MinIO.
    """

    destination = (
        f"s3://{settings.minio_bucket_bronze}"
        f"/olist/{table}/"
        f"ingestion_date={ingested_at.date()}/"
        f"{table}.parquet"
    )

    logger.info("writing_bronze", destination=destination, rows=len(df))

    df.to_parquet(
        destination,
        engine="pyarrow",
        index=False,
        storage_options={
            "key": settings.minio_root_user,
            "secret": settings.minio_root_password,
            "client_kwargs": {"endpoint_url": settings.minio_endpoint},
        },
    )

    return destination


def ingest_table(table: str):
    """
    Ejecuta la ingesta completa de una tabla.
    """

    start = time.perf_counter()

    ingested_at = datetime.now(UTC)

    batch_id = str(uuid5(NAMESPACE_DNS, f"{table}-{ingested_at.date()}"))

    bind_contextvars(table=table, batch_id=batch_id)

    try:
        logger.info("ingestion_started")

        df, source_file = read_csv(table)

        logger.info("csv_loaded", rows=len(df))

        df = add_metadata(df, source_file, ingested_at, batch_id)

        destination = write_bronze(df, ingested_at, table)

        duration = time.perf_counter() - start

        logger.info(
            "ingestion_finished",
            rows_written=len(df),
            destination=destination,
            duration_seconds=duration,
        )

    except Exception as error:
        logger.exception(
            "ingestion_failed", error_type=type(error).__name__, error_message=str(error)
        )

        raise


def main():
    configure_logging()

    bind_contextvars(run_id=str(uuid4()))

    args = parse_args()

    tables = get_tables(args.table)

    for table in tables:
        ingest_table(table)


if __name__ == "__main__":
    main()
