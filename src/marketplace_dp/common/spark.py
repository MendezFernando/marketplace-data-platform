"""Fábrica de sesiones de Spark configuradas para el lakehouse local.

Centraliza toda la configuración de Spark en un único sitio para que los trabajos
de transformación no repitan credenciales, endpoints ni coordenadas de JARs.
"""

from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from marketplace_dp.common.config import PROJECT_ROOT, settings

# ─────────────────────────────────────────────────────────────────────────────
#  Versiones de los JARs de Hadoop para hablar S3A con MinIO.
#  DEBEN coincidir con la versión de Hadoop que embebe PySpark (3.5.x → 3.3.4).
#  Un desajuste aquí produce NoClassDefFoundError o NoSuchMethodError en runtime,
#  que es de los errores más difíciles de diagnosticar del ecosistema Spark.
# ─────────────────────────────────────────────────────────────────────────────
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_BUNDLE_VERSION = "1.12.262"

# Driver JDBC de PostgreSQL: necesario para materializar Gold en el warehouse,
# que es donde dbt, Power BI y la API leerán.
POSTGRES_JDBC_VERSION = "42.7.4"


def get_spark(app_name: str = "marketplace-dp") -> SparkSession:
    """Devuelve una SparkSession con Delta Lake y acceso S3A a MinIO."""

    # Spark lanza la JVM leyendo JAVA_HOME del entorno del proceso.
    os.environ["JAVA_HOME"] = str(settings.java_home)
    # Fuerza que los workers usen el mismo intérprete del venv que el driver.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # En Windows, Hadoop necesita binarios nativos (winutils.exe / hadoop.dll)
    # para traducir operaciones POSIX. Sin esto Spark no arranca el contexto.
    # En Linux y macOS este bloque no aplica y se ignora.
    if sys.platform == "win32":
        hadoop_home = PROJECT_ROOT / "infra" / "hadoop"
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        os.environ["PATH"] = f"{hadoop_home / 'bin'};{os.environ['PATH']}"

    builder = (
        SparkSession.builder.appName(app_name)
        # local[*] = modo local usando todos los núcleos disponibles.
        # En producción esto sería "yarn" o "k8s://..." y NADA MÁS cambiaría.
        .master("local[*]")
        .config("spark.driver.memory", settings.spark_driver_memory)
        # ─── Delta Lake ──────────────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ─── S3A apuntando a MinIO ───────────────────────────────────────
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_root_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_root_password)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # MinIO no soporta el direccionamiento por subdominio de AWS
        # (bucket.s3.amazonaws.com): exige rutas del tipo host/bucket.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ─── Ajustes para ejecución local ────────────────────────────────
        # Por defecto Spark usa 200 particiones de shuffle, pensadas para un
        # clúster. En local eso genera 200 tareas minúsculas y mucha sobrecarga.
        .config("spark.sql.shuffle.partitions", "8")
        # Todo el procesamiento en UTC, coherente con la capa Bronze.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    )

    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_BUNDLE_VERSION}",
            f"org.postgresql:postgresql:{POSTGRES_JDBC_VERSION}",
        ],
    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
