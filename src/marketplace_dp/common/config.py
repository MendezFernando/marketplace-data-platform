from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    # ─── Almacenamiento de objetos (MinIO en local, S3 en AWS) ──────────
    # Los nombres son neutrales a propósito: el código no debe saber qué
    # implementación hay detrás, solo que habla la API de S3.
    # `AliasChoices` acepta las variables nuevas (S3_*) y las antiguas
    # (MINIO_*), de modo que el .env existente sigue funcionando.
    s3_access_key: str = Field(
        validation_alias=AliasChoices("S3_ACCESS_KEY", "MINIO_ROOT_USER"),
    )
    s3_secret_key: str = Field(
        validation_alias=AliasChoices("S3_SECRET_KEY", "MINIO_ROOT_PASSWORD"),
    )

    # Endpoint VACÍO = AWS S3 real (el SDK resuelve el host por región).
    # Con valor = MinIO u otro servicio compatible.
    s3_endpoint: str = Field(
        default="",
        validation_alias=AliasChoices("S3_ENDPOINT", "MINIO_ENDPOINT"),
    )
    s3_region: str = "us-east-1"

    bucket_bronze: str = Field(
        validation_alias=AliasChoices("BUCKET_BRONZE", "MINIO_BUCKET_BRONZE"),
    )
    bucket_silver: str = Field(
        validation_alias=AliasChoices("BUCKET_SILVER", "MINIO_BUCKET_SILVER"),
    )
    bucket_gold: str = Field(
        validation_alias=AliasChoices("BUCKET_GOLD", "MINIO_BUCKET_GOLD"),
    )

    @property
    def use_custom_endpoint(self) -> bool:
        """True si el almacenamiento NO es AWS S3 (p. ej. MinIO local)."""
        return bool(self.s3_endpoint)

    @property
    def s3_storage_options(self) -> dict:
        """Opciones para pandas / s3fs, en ambos entornos."""
        options: dict = {"key": self.s3_access_key, "secret": self.s3_secret_key}
        if self.use_custom_endpoint:
            options["client_kwargs"] = {"endpoint_url": self.s3_endpoint}
        else:
            options["client_kwargs"] = {"region_name": self.s3_region}
        return options

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
