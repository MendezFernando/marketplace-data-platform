"""Configuración compartida por todas las pruebas.

`conftest.py` es un archivo especial de pytest: lo que se define aquí está
disponible en todos los tests sin necesidad de importarlo.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Sesión de Spark mínima, compartida por toda la corrida de pruebas.

    `scope="session"` es deliberado: arrancar la JVM tarda varios segundos, así
    que se hace UNA vez y no una por test.

    Esta sesión no lleva Delta, ni S3, ni credenciales. Las pruebas unitarias
    verifican LÓGICA, no infraestructura: si necesitaran red o disco serían
    lentas, dependerían de que un servicio esté encendido y fallarían por
    motivos que no tienen nada que ver con el código bajo prueba.
    """
    session = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        # Datos diminutos: 200 particiones de shuffle solo añadirían sobrecarga.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session  # todo lo anterior es preparación; lo siguiente, limpieza

    session.stop()
