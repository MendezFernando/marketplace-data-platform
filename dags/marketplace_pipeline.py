"""
Pipeline diario de la plataforma de datos del marketplace.

Flujo:
    Bronze -> Silver -> dimensiones -> hechos -> PostgreSQL -> dbt

Todas las tareas trabajan sobre la FECHA LÓGICA de la corrida (`{{ ds }}`), no
sobre el reloj: eso es lo que permite reprocesar un día pasado y obtener el
mismo resultado.

Notas de diseño que valen más que el código:

* `max_active_runs=1`  : nunca dos días a la vez. `dim_seller` guarda historia
                         (SCD Tipo 2) y procesar fechas fuera de orden la
                         corrompe en silencio.
* `max_active_tasks=2` : cada tarea levanta su propia JVM de Spark (2 GB). Más
                         paralelismo del que aguanta Docker termina en procesos
                         muertos por falta de memoria (OOMKilled, exit -9).
* `depends_on_past`    : solo en `dim_seller`, por el mismo motivo. En el resto
                         estorba: un fallo congelaría todo el calendario.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ─────────────────────────────────────────────────────────────────────────────
#  Constantes
# ─────────────────────────────────────────────────────────────────────────────

# El código del proyecto se monta en /opt/project dentro del contenedor.
# El `cd` es obligatorio: `python -m` resuelve el paquete desde el directorio
# actual, y las rutas relativas del proyecto (data/landing) cuelgan de ahí.
PROJECT = "cd /opt/project && python -m marketplace_dp"

# Dimensiones que se reconstruyen completas desde Silver (sobrescriben).
# `dim_seller` NO está aquí: usa MERGE con historial y se trata aparte.
DIMENSIONES = ["dim_date", "dim_customer", "dim_product", "dim_geography"]

# Tablas de hechos. Todas necesitan las dimensiones ya publicadas para
# resolver sus llaves sustitutas.
HECHOS = ["fct_orders", "fct_payments", "fct_reviews", "fct_order_items"]

default_args = {
    "owner": "fernando",
    # La mayoría de fallos en datos son temporales (red, memoria, bloqueos).
    # Reintentar es seguro porque cada job sobrescribe su propia salida.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Si una tarea se cuelga, se mata en lugar de bloquear el pipeline para siempre.
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id="marketplace_pipeline",
    description="Olist: Bronze -> Silver -> Gold -> PostgreSQL -> dbt",
    # Los datos de Olist son de 2016-2018; la fecha de inicio permite disparar
    # corridas con fechas lógicas de ese periodo.
    start_date=datetime(2016, 1, 1),
    schedule="@daily",
    # Sin catchup: al activarlo no se lanzan cientos de corridas históricas.
    # Los días pasados se piden a propósito con un backfill.
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args=default_args,
    tags=["olist", "medallion", "spark", "dbt"],
) as dag:

    # ─── Bronze ──────────────────────────────────────────────────────────────
    # Idempotente: reescribe la partición ingestion_date={{ ds }} completa.
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{PROJECT}.ingestion.csv_to_bronze --table all --ingestion-date {{{{ ds }}}}",
        doc_md="Carga los CSV de landing a Bronze, en la partición del día lógico.",
    )

    # ─── Silver ──────────────────────────────────────────────────────────────
    # Un solo task para las 8 entidades: son rápidas y así se evita levantar
    # ocho sesiones de Spark que competirían por la memoria del contenedor.
    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"{PROJECT}.silver_transformation.run_all_silver --ingestion-date {{{{ ds }}}}"
        ),
        doc_md="Limpia, tipa y deduplica las 8 entidades de negocio sobre Delta Lake.",
    )

    # ─── Dimensiones ─────────────────────────────────────────────────────────
    # Se crean en un bucle: ocho bloques casi iguales son ocho sitios donde
    # equivocarse. No reciben fecha porque se reconstruyen enteras desde Silver.
    dimensiones = [
        BashOperator(
            task_id=nombre,
            bash_command=f"{PROJECT}.gold_transformation.run_all_gold --entity {nombre}",
        )
        for nombre in DIMENSIONES
    ]

    # `dim_seller` aparte: SCD Tipo 2 cargado con MERGE.
    #   * `--as-of {{ ds }}` fija la ventana de GMV y la vigencia de las versiones.
    #   * `depends_on_past=True`: el día N no arranca si el día N-1 no terminó
    #     bien. Aplicar una fecha anterior a la última procesada cierra versiones
    #     con rangos invertidos y deja la versión vigente equivocada, sin error.
    dim_seller = BashOperator(
        task_id="dim_seller",
        bash_command=f"{PROJECT}.gold_transformation.dim_seller --as-of {{{{ ds }}}}",
        depends_on_past=True,
        doc_md="Dimensión con historial (SCD Tipo 2). Sensible al orden de ejecución.",
    )

    # Punto de sincronización: hace legible el grafo y garantiza que NINGÚN
    # hecho empiece antes de que TODAS las dimensiones estén publicadas.
    # Sin él harían falta 5 x 4 = 20 flechas.
    dimensiones_listas = EmptyOperator(task_id="dimensiones_listas")

    # ─── Hechos ──────────────────────────────────────────────────────────────
    hechos = [
        BashOperator(
            task_id=nombre,
            bash_command=f"{PROJECT}.gold_transformation.run_all_gold --entity {nombre}",
        )
        for nombre in HECHOS
    ]

    # ─── Serving ─────────────────────────────────────────────────────────────
    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"{PROJECT}.serving.gold_to_postgres",
        doc_md="Materializa Gold en el esquema `gold` de PostgreSQL vía JDBC.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/project && python scripts/run_dbt.py run",
        doc_md="Construye los marts de negocio sobre el esquema `gold`.",
    )

    # Test va DESPUÉS de run, no antes: valida lo que se acaba de publicar.
    # Si falla, la corrida queda en rojo y el problema se ve el mismo día.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/project && python scripts/run_dbt.py test",
        doc_md="30 pruebas de calidad: unicidad, integridad referencial y reglas de negocio.",
    )

    # ─── Dependencias ────────────────────────────────────────────────────────
    # Regla: A >> B solo si B lee lo que A escribe.
    ingest_bronze >> build_silver

    # Las dimensiones leen Silver y ninguna lee a otra: van en paralelo
    # (limitadas por max_active_tasks, no por el grafo).
    build_silver >> [*dimensiones, dim_seller] >> dimensiones_listas

    # Los hechos resuelven llaves contra TODAS las dimensiones.
    dimensiones_listas >> hechos

    # `load_postgres` publica las tablas de Gold: espera a todos los hechos.
    hechos >> load_postgres >> dbt_run >> dbt_test
