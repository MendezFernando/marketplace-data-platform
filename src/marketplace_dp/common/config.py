from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    # ─── MinIO ────────────────────────────────────────────────
    minio_endpoint: str
    minio_root_user: str
    minio_root_password: str

    minio_bucket_bronze: str
    minio_bucket_silver: str
    minio_bucket_gold: str

    # ─── PostgreSQL (Data Warehouse / capa de servicio) ──────
    postgres_host: str
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_password: str

    # ─── Rutas locales ───────────────────────────────────────
    landing_path: Path = PROJECT_ROOT / "data" / "landing"

    # ─── Spark ───────────────────────────────────────────────
    # Spark corre sobre la JVM: necesita saber qué JDK usar. Se declara
    # explícitamente para no depender del JAVA_HOME global de la máquina.
    java_home: Path
    spark_driver_memory: str = "4g"

    # Configuración de lectura del .env
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Instancia única para usar en todo el proyecto
settings = Settings()
