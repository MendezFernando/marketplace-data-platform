"""API de la plataforma de datos del marketplace.

Entrega las métricas de negocio (los marts de dbt) a quien las pida por
internet: un dashboard, otra aplicación o un navegador.

Levantar:    uvicorn marketplace_dp.api.main:app --reload --port 8000
Probar:      http://localhost:8000/docs
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query
from psycopg import Connection

from marketplace_dp.api.db import MARTS_SCHEMA, get_connection, pool
from marketplace_dp.api.schemas import (
    CohortesResponse,
    GmvSegmentoResponse,
    Meta,
    SatisfaccionResponse,
    Tramo,
)

# Formato de mes aceptado: YYYY-MM (01 a 12).
YEAR_MONTH = r"^\d{4}-(0[1-9]|1[0-2])$"


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Al arrancar abre las conexiones a la base; al apagar, las cierra."""
    pool.open()
    yield
    pool.close()


app = FastAPI(
    title="Marketplace Data Platform API",
    description="Métricas de negocio del marketplace Olist, servidas desde los marts de dbt.",
    version="1.0.0",
    lifespan=lifespan,
)


def _meta(rows: list[dict], limit: int, offset: int) -> Meta:
    """El total viene en cada fila (COUNT(*) OVER ()); si no hay filas, es 0."""
    return Meta(total=rows[0]["total"] if rows else 0, limit=limit, offset=offset)


@app.get("/health", tags=["operación"])
def health(conn: Connection = Depends(get_connection)) -> dict:
    """¿Está viva la API y alcanza la base de datos?"""
    try:
        conn.execute("SELECT 1")
    except Exception as error:
        raise HTTPException(status_code=503, detail="Base de datos no disponible") from error
    return {"status": "ok", "database": "ok"}


@app.get("/v1/gmv-por-segmento", response_model=GmvSegmentoResponse, tags=["ventas"])
def gmv_por_segmento(
    desde: str | None = Query(None, pattern=YEAR_MONTH, description="Mes inicial, YYYY-MM"),
    hasta: str | None = Query(None, pattern=YEAR_MONTH, description="Mes final, YYYY-MM"),
    limit: int = Query(100, ge=1, le=1000, description="Filas por página (máx. 1000)"),
    offset: int = Query(0, ge=0, description="Filas a saltar"),
    conn: Connection = Depends(get_connection),
) -> GmvSegmentoResponse:
    """Ventas mensuales por segmento de vendedor.

    El segmento es el que tenía el vendedor EL DÍA DE LA VENTA (SCD Tipo 2).
    """
    if desde and hasta and desde > hasta:
        raise HTTPException(status_code=400, detail="'desde' no puede ser posterior a 'hasta'")

    # Los valores del usuario van SIEMPRE como parámetros (%(...)s), nunca
    # pegados al texto del SQL: así nadie puede colar instrucciones propias.
    # El `::text` le dice a Postgres el tipo cuando el filtro llega vacío.
    rows = conn.execute(
        f"""
        SELECT year_month, seller_segment, gmv, items_vendidos, ordenes,
               COUNT(*) OVER () AS total
        FROM {MARTS_SCHEMA}.mart_segmento_vendedor
        WHERE (%(desde)s::text IS NULL OR year_month >= %(desde)s::text)
          AND (%(hasta)s::text IS NULL OR year_month <= %(hasta)s::text)
        ORDER BY year_month, seller_segment
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {"desde": desde, "hasta": hasta, "limit": limit, "offset": offset},
    ).fetchall()

    return GmvSegmentoResponse(data=rows, meta=_meta(rows, limit, offset))


@app.get("/v1/cohortes", response_model=CohortesResponse, tags=["clientes"])
def cohortes(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_connection),
) -> CohortesResponse:
    """Retención por cohorte: de los clientes que compraron por primera vez en
    un mes, cuántos volvieron a comprar a los 3, 6 y 12 meses."""
    rows = conn.execute(
        f"""
        SELECT cohort_year_month, clientes_cohorte, recompra_3m, recompra_6m,
               recompra_12m, gmv_acumulado, gmv_primera_compra,
               COUNT(*) OVER () AS total
        FROM {MARTS_SCHEMA}.mart_cohortes
        ORDER BY cohort_year_month
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {"limit": limit, "offset": offset},
    ).fetchall()

    return CohortesResponse(data=rows, meta=_meta(rows, limit, offset))


@app.get("/v1/satisfaccion", response_model=SatisfaccionResponse, tags=["clientes"])
def satisfaccion(
    tramo: Tramo | None = Query(None, description="Filtra por tramo de retraso de entrega"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_connection),
) -> SatisfaccionResponse:
    """Calificación de los clientes según cuánto se retrasó su entrega."""
    rows = conn.execute(
        f"""
        SELECT year_month, tramo_retraso, reviews, reviews_negativas,
               reviews_positivas, score_medio, tiempo_respuesta_medio_h,
               retraso_medio_dias,
               COUNT(*) OVER () AS total
        FROM {MARTS_SCHEMA}.mart_satisfaccion
        WHERE (%(tramo)s::text IS NULL OR tramo_retraso = %(tramo)s::text)
        ORDER BY year_month, tramo_retraso
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {"tramo": tramo, "limit": limit, "offset": offset},
    ).fetchall()

    return SatisfaccionResponse(data=rows, meta=_meta(rows, limit, offset))
