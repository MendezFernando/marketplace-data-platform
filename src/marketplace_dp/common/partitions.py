import re
from datetime import date

import s3fs

from marketplace_dp.common.config import settings


def resolve_ingestion_date(table: str, requested: str | None) -> date:
    """
    Resuelve la fecha de ingesta a utilizar.

    - Si requested existe: valida que la partición exista.
    - Si requested es None: toma la última partición disponible.
    """

    fs = s3fs.S3FileSystem(
        **settings.s3_storage_options,
    )

    path = f"{settings.bucket_bronze}/olist/{table}/"

    folders = fs.ls(path)

    available_dates = []

    for folder in folders:
        match = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", folder)

        if match:
            available_dates.append(date.fromisoformat(match.group(1)))

    if not available_dates:
        raise ValueError(f"No existen particiones para {table}")

    if requested is not None:
        requested_date = date.fromisoformat(requested)

        if requested_date not in available_dates:
            raise ValueError(f"La partición {requested} no existe para {table}")

        return requested_date

    return max(available_dates)
