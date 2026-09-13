# Imagen de Airflow con el paquete del proyecto y Java instalados.
#
# Airflow no ejecuta scripts de tu Windows: solo puede ejecutar lo que hay dentro
# del contenedor. Por eso el pipeline se empaqueta AQUÍ, que es además como
# funciona en producción: el código de datos siempre viaja en una imagen.
#
# Efecto secundario muy bienvenido: dentro de Linux, Hadoop encuentra las APIs
# POSIX que espera, así que `winutils.exe` y `hadoop.dll` dejan de hacer falta.

FROM apache/airflow:2.10.4-python3.11

USER root

# Java es requisito de Spark: corre sobre la JVM.
# procps aporta `ps`, que Spark usa para consultar el uso de memoria.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Las dependencias se instalan ANTES de copiar el código: Docker cachea cada capa,
# así que cambiar un .py no obliga a reinstalar PySpark (400 MB) cada vez.
COPY --chown=airflow:root pyproject.toml /opt/project/pyproject.toml
COPY --chown=airflow:root README.md /opt/project/README.md
RUN mkdir -p /opt/project/src/marketplace_dp \
    && touch /opt/project/src/marketplace_dp/__init__.py \
    && pip install --no-cache-dir -e /opt/project \
    && pip install --no-cache-dir "dbt-core>=1.8,<2.0" "dbt-postgres>=1.8,<2.0"

# El código real se monta como volumen en docker-compose para poder editarlo sin
# reconstruir la imagen. En un despliegue de verdad se copiaría aquí con COPY.
