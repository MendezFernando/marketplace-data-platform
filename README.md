# Marketplace Data Platform

[![CI](https://github.com/MendezFernando/marketplace-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/MendezFernando/marketplace-data-platform/actions/workflows/ci.yml)

Lakehouse de extremo a extremo sobre el dataset real de
[Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): ingesta idempotente,
arquitectura medallón sobre Delta Lake, modelado dimensional con historial, orquestación diaria y
pruebas automáticas en cada cambio.

**El objetivo no fue mover datos: fue que se puedan reprocesar, verificar y explicar.**

![DAG diario de Airflow](docs/img/airflow-dag.png)

---

## El problema de ingeniería

Un pipeline que "funciona" es fácil. Los tres problemas que este proyecto resuelve son los que
aparecen cuando algo sale mal:

| Problema | Qué se construyó |
|---|---|
| **Un día falla y hay que reprocesarlo tres días después** | Cada trabajo se parametriza por fecha lógica y sobrescribe su propia salida: re-ejecutar produce exactamente el mismo resultado |
| **La lógica de negocio tenía un error y ya está en producción** | Bronze es inmutable; Silver y Gold se reconstruyen desde él sin volver a pedir datos al origen |
| **Los números cambian y nadie sabe por qué** | 33 pruebas de datos y 22 unitarias que corren solas, más historial versionado en el modelo dimensional |

---

## Arquitectura

```
  FUENTES            INGESTA                LAKEHOUSE (Delta Lake sobre S3)            SERVICIO            CONSUMO
┌───────────┐     ┌──────────┐     ┌──────────────────────────────────────────┐   ┌─────────────┐   ┌──────────────┐
│ 9 CSV     │────▶│  Python  │────▶│ BRONZE   crudo, inmutable, particionado  │   │             │──▶│ dbt marts    │
│ Olist     │     │  pandas  │     │          por fecha lógica                │   │ PostgreSQL  │   ├──────────────┤
│ 1.5M      │     └──────────┘     ├──────────────────────────────────────────┤   │  esquema    │──▶│ FastAPI      │
│ filas     │                      │ SILVER   limpio, tipado, deduplicado     │──▶│  gold       │   ├──────────────┤
└───────────┘     ┌──────────┐     │          566,358 filas · 8 entidades     │   │             │──▶│ Power BI     │
                  │ PySpark  │────▶├──────────────────────────────────────────┤   └─────────────┘   └──────────────┘
                  │ + Delta  │     │ GOLD     esquema estrella Kimball        │
                  └──────────┘     │          5 dimensiones + 4 hechos, SCD2  │──▶ Glue Catalog ──▶ Athena (SQL ad hoc)
                                   └──────────────────────────────────────────┘

  ORQUESTACIÓN  Apache Airflow · DAG diario de 14 tareas, reintentos y backfill
  CALIDAD       33 pruebas de datos (dbt) + 22 pruebas unitarias (pytest)
  CI/CD         GitHub Actions · lint y pruebas en cada Pull Request
  NUBE          AWS S3 + Glue Data Catalog + Athena, IAM de privilegio mínimo
  EJECUCIÓN     Docker Compose (PostgreSQL, MinIO, Airflow)
```

📄 [ADR-0001 — Por qué lakehouse medallón y no un DWH clásico](docs/adr/0001-arquitectura-lakehouse-medallon.md)

---

## Decisiones de ingeniería

| Decisión | Por qué |
|---|---|
| **Idempotencia en todos los trabajos** | Cada job se parametriza por fecha y sobrescribe su propia salida. Es el prerrequisito de los reintentos y del backfill. |
| **Bronze inmutable y particionado** | Es la póliza de seguro: un error de lógica nunca obliga a volver a pedir datos al origen. |
| **Fecha lógica, nunca el reloj** | Reprocesar el martes desde el jueves debe escribir en la partición del martes. Sin esto, el backfill produce huecos o duplicados. |
| **`decimal(12,2)` para dinero, nunca float** | El binario no representa decimales exactos: sobre millones de filas aparecen descuadres de céntimos que rompen la conciliación. |
| **SCD Tipo 2 en `dim_seller` con MERGE** | Cada venta conserva el segmento vigente **ese día**, vía join point-in-time. Los informes históricos son reproducibles. |
| **Miembros desconocidos (`-1`)** | Una clave sin resolver se ve como "Unknown" en vez de evaporarse en el siguiente join. |
| **Nunca almacenar ratios** | Se guardan numerador y denominador; los porcentajes se calculan al consultar y siguen siendo correctos a cualquier nivel de agregación. |
| **`depends_on_past` solo en el SCD2** | Procesar fechas fuera de orden corrompe el historial. En el resto de tareas solo congelaría el calendario sin necesidad. |
| **Código desacoplado del proveedor** | El proyecto habla la API de S3, no con MinIO. Migrar a AWS fue cambiar variables de entorno, sin tocar Python. |
| **IAM de privilegio mínimo, verificado** | Se retiró `AdministratorAccess` y se comprobó en ambos sentidos: el pipeline funciona; EC2, borrar buckets y crear usuarios quedan denegados. |

---

## Las tres capas

**Bronze — lo que llegó.** Ingesta idempotente a Parquet particionado por fecha lógica, con
columnas de auditoría en cada fila (`_ingested_at`, `_source_file`, `_batch_id`) y logs
estructurados correlacionados por `run_id`.

**Silver — lo que es cierto.** Ocho entidades de negocio construidas con PySpark sobre Delta Lake:
tipado explícito, normalización de texto y códigos, y resolución de identidad del cliente
(99,441 identificadores a nivel orden → 96,096 personas reales). La clave primaria se valida
**antes** de publicar y el esquema se hace cumplir en la escritura.

**Gold — lo que significa.** Esquema estrella de Kimball: cinco dimensiones conformadas y cuatro
tablas de hechos a distintos granos, que nunca se unen entre sí. `dim_seller` es SCD Tipo 2
cargada con MERGE, resolviendo con una segunda fila de clave nula el problema de que un MERGE solo
dispara una acción por coincidencia cuando un cambio necesita dos: cerrar la versión anterior y
abrir la nueva.

| Catálogo · 9 tablas Delta en AWS Glue | SQL sin servidores sobre el lakehouse |
|---|---|
| ![Glue Data Catalog](docs/img/aws-glue.png) | ![Athena](docs/img/aws-athena.png) |

La consulta de Athena devuelve 1,274 filas en 2.6 s escaneando 11.55 MB: en formato columnar, el
costo lo determinan las **columnas leídas**, no las filas devueltas.

---

## Calidad de datos

Dos tipos de prueba con propósitos distintos, y la diferencia importa:

**33 pruebas de datos (dbt)** — con los datos reales, en cada carga: unicidad, integridad
referencial, valores aceptados y reglas de negocio propias (por ejemplo, que los clientes que
recompran nunca superen el tamaño de su cohorte). Tres protegen la integridad del SCD Tipo 2:
una sola versión vigente por vendedor, sin rangos invertidos y sin traslapes.

**22 pruebas unitarias (pytest)** — con datos sintéticos, en cada Pull Request. Varias son de
regresión: blindan errores que ya costó encontrar una vez, como que una fecha nula se resuelva al
miembro desconocido en lugar de hacer desaparecer la fila al unir con `dim_date`.

![Linaje de dbt](docs/img/dbt-lineage.png)

```bash
pytest tests/unit -v            # 22 pruebas de código
python scripts/run_dbt.py test  # 33 pruebas de datos
```

---

## Cómo ejecutarlo

**Requisitos:** Docker Desktop, Python 3.11, Java 17, y los CSV de Olist en `data/landing/`.

```bash
# 1. Configuración
cp .env.example .env          # completar credenciales locales

# 2. Servicios (PostgreSQL, MinIO, Airflow)
docker compose up -d

# 3. Pipeline completo, un día de datos
python -m marketplace_dp.ingestion.csv_to_bronze --table all --ingestion-date 2018-09-04
python -m marketplace_dp.silver_transformation.run_all_silver --ingestion-date 2018-09-04
python -m marketplace_dp.gold_transformation.dim_seller --as-of 2018-09-04
python -m marketplace_dp.gold_transformation.run_all_gold
python -m marketplace_dp.serving.gold_to_postgres
python scripts/run_dbt.py build

# 4. O todo orquestado: http://localhost:8080  (admin/admin)
#    Disparar `marketplace_pipeline` con fecha lógica 2018-09-04
```

**Para ejecutar contra AWS en lugar de MinIO**, solo cambia el `.env`:

```diff
- S3_ENDPOINT=http://localhost:9000     # MinIO local
+ S3_ENDPOINT=                          # vacío = AWS S3
```

---

## Consumo

El modelo se sirve por tres vías, según quién pregunte:

**API REST (FastAPI)** para aplicaciones: validación de parámetros, paginación con tope máximo,
SQL parametrizado y documentación OpenAPI generada automáticamente.

```bash
uvicorn marketplace_dp.api.main:app --reload --port 8000   # http://localhost:8000/docs
```

![API](docs/img/api-docs.png)

**Athena** para consultas ad hoc sobre el lakehouse, sin infraestructura encendida.

**Power BI** para el negocio, conectado a los marts de dbt.

![Dashboard](docs/img/powerbi.png)

Lo que el modelo permitió responder:

| Hallazgo | Dato | Implicación |
|---|---|---|
| El Nordeste incumple la promesa de entrega | **14.3%** de entregas tardías, contra 7.0% en Sul | Estimar la fecha de entrega por región |
| El retraso destruye la satisfacción | La calificación cae de **4.3** (a tiempo) a **1.7** (>7 días) | La logística es un problema de producto, no solo de costo |
| Negocio de adquisición, no de retención | Solo **~3%** de clientes recompra en 12 meses | El gasto en captación no se recupera con recurrencia |

---

## Estructura

```
src/marketplace_dp/
    common/                  configuración, Spark, lakehouse, logging
    ingestion/               CSV → Bronze (idempotente, por fecha lógica)
    silver_transformation/   8 entidades de negocio limpias
    gold_transformation/     5 dimensiones + 4 tablas de hechos
    serving/                 Gold → PostgreSQL
    api/                     FastAPI sobre los marts
dags/                        DAG diario de Airflow
dbt/                         marts de negocio y pruebas de datos
tests/unit/                  pruebas unitarias
docs/                        requisitos, diseño y decisiones
infra/                       Dockerfile de Airflow, init de PostgreSQL
```

---

## Documentación

| Documento | Contenido |
|---|---|
| [Requisitos de negocio](docs/00-business-requirements.md) | Preguntas de negocio, métricas y SLAs |
| [ADR-0001](docs/adr/0001-arquitectura-lakehouse-medallon.md) | Por qué lakehouse medallón y no un DWH clásico |
| [Diseño de Silver](docs/01-silver-design.md) | Reglas de limpieza y tipado por entidad |
| [Modelo dimensional](docs/02-gold-dimensional-model.md) | Grano, claves, SCD2 y verificación |
| [Migración a AWS](docs/03-aws-cloud.md) | S3, Glue, Athena, costos y privilegio mínimo |

---

## Pendiente

- Streaming con Kafka y Spark Structured Streaming
- Observabilidad: alertas de frescura y detección de volúmenes anómalos
- Ejecución del pipeline dentro de AWS (Glue Jobs o MWAA) e infraestructura como código

---

Datos: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (licencia CC BY-NC-SA 4.0).
Proyecto construido por [Fernando Méndez](https://mendezfernando.github.io).
