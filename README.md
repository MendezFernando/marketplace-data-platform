# Marketplace Data Platform

> Plataforma de datos end-to-end para un marketplace de e-commerce: ingesta multi-fuente,
> arquitectura Bronze/Silver/Gold, modelado dimensional y entrega a dashboards y APIs.

> ⚠️ **Proyecto en construcción.** Este README se completa en el Módulo 12.

## Problema de negocio

El marketplace toma decisiones con exports manuales de la base transaccional: sin historia, sin
métricas consistentes y con cada área calculando números distintos. Esta plataforma centraliza,
versiona y valida los datos para responder preguntas de Finanzas, Growth, Operaciones y Producto.

📄 **Requisitos y SLAs:** [`docs/00-business-requirements.md`](docs/00-business-requirements.md)

## Arquitectura

```
   FUENTES              INGESTA            LAKEHOUSE (MinIO/S3)        WAREHOUSE      CONSUMO
┌────────────┐      ┌───────────┐     ┌──────────────────────────┐  ┌───────────┐  ┌──────────┐
│ CSV Olist  │─────▶│           │────▶│ BRONZE  crudo, inmutable │  │           │  │ Power BI │
│ API FX     │─────▶│  Python   │     │   Parquet particionado   │  │           │─▶│          │
│ API clima  │─────▶│  ingest   │     ├──────────────────────────┤  │ PostgreSQL│  ├──────────┤
│ Clickstream│──┐   └───────────┘     │ SILVER  limpio, tipado   │─▶│   (DWH)   │  │ FastAPI  │
└────────────┘  │                     │   Delta Lake, dedup, SCD │  │           │─▶│          │
                │   ┌───────────┐     ├──────────────────────────┤  │ star      │  ├──────────┤
                └──▶│  Spark    │────▶│ GOLD  agregados negocio  │  │ schema    │─▶│ notebooks│
                    │ Streaming │     └──────────────────────────┘  └───────────┘  └──────────┘
                    └───────────┘                  ▲                       ▲
                                                   │                       │
                              PySpark + Delta ─────┘                   dbt Core

   ORQUESTA: Apache Airflow  │  CALIDAD: dbt tests + Great Expectations  │  Docker Compose
   CI/CD: GitHub Actions     │  OBSERVABILIDAD: logs estructurados + métricas + alertas
```

## Stack

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.11 |
| Data Lake | MinIO (S3-compatible), Parquet, Delta Lake |
| Procesamiento | PySpark |
| Data Warehouse | PostgreSQL |
| Transformación | dbt Core |
| Orquestación | Apache Airflow |
| Calidad de datos | dbt tests, Great Expectations |
| Streaming | Redpanda + Spark Structured Streaming |
| Serving | FastAPI, Power BI |
| Infraestructura | Docker, Docker Compose |
| CI/CD | GitHub Actions, pytest, Ruff, pre-commit |

## Estructura del repositorio

```
├── .github/workflows/   Pipelines de CI/CD
├── dags/                DAGs de Airflow (solo orquestación, sin lógica de negocio)
├── dbt/                 Proyecto dbt: modelos Silver → Gold, tests y documentación
├── docs/                Requisitos, decisiones de arquitectura (ADR) y diagramas
├── infra/               Dockerfiles y ficheros de configuración de los servicios
├── notebooks/           Exploración ad-hoc — NUNCA código de producción
├── src/marketplace_dp/  Paquete Python: ingesta, transformaciones y utilidades
├── tests/               Pruebas unitarias y de integración (espejo de src/)
└── docker-compose.yml   Definición de la plataforma local
```

## Puesta en marcha

**Requisitos:** Docker Desktop con backend WSL 2.

```bash
cp .env.example .env      # y rellena las credenciales locales
docker compose up -d      # levanta PostgreSQL + MinIO y crea los buckets
docker compose ps         # todos los servicios deben estar "healthy"
```

| Servicio | URL | Uso |
|---|---|---|
| PostgreSQL | `localhost:5432` | Data Warehouse |
| MinIO — API S3 | `localhost:9000` | Lo consumen Spark, Python y dbt |
| MinIO — Consola | http://localhost:9001 | Interfaz web para inspeccionar el lake |

```bash
docker compose down       # apaga (conserva los datos)
docker compose down -v    # apaga y BORRA los volúmenes
```

## Licencia

MIT
