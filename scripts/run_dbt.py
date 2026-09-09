"""Ejecuta dbt cargando antes las variables de entorno del proyecto.

`profiles.yml` resuelve todas las credenciales con `env_var()`, así que no
contiene secretos y puede versionarse — pero dbt no lee `.env` por su cuenta.
Este envoltorio carga el `.env` y delega en la CLI de dbt.

    python scripts/run_dbt.py debug
    python scripts/run_dbt.py run
    python scripts/run_dbt.py test
    python scripts/run_dbt.py docs generate
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    # dbt busca profiles.yml en ~/.dbt por defecto; aquí vive junto al proyecto
    # para que la configuración viaje con el repositorio.
    os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

    from dbt.cli.main import cli

    # Se deja que la CLI de dbt gestione sus propios errores y su salida: ya los
    # formatea de forma legible. Envolverlos aquí solo sepultaría el mensaje útil.
    cli.main(args=[*sys.argv[1:], "--project-dir", str(DBT_DIR)])
    return 0


if __name__ == "__main__":
    sys.exit(main())
