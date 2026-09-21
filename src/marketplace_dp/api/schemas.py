"""Forma de las respuestas de la API.

Cada clase describe exactamente qué campos devuelve un endpoint. Es el
"contrato" con quien consume la API: si cambia, se publica una nueva versión.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

Tramo = Literal["a tiempo", "1-3 dias", "4-7 dias", ">7 dias"]


class Meta(BaseModel):
    """Información de la paginación."""

    total: int = Field(description="Filas que cumplen el filtro, sin paginar")
    limit: int
    offset: int


class GmvSegmento(BaseModel):
    year_month: str = Field(examples=["2018-01"])
    seller_segment: str = Field(examples=["Gold"])
    gmv: float = Field(description="Ventas en BRL del segmento vigente ese mes")
    items_vendidos: int
    ordenes: int


class GmvSegmentoResponse(BaseModel):
    data: list[GmvSegmento]
    meta: Meta


class Cohorte(BaseModel):
    cohort_year_month: str = Field(examples=["2017-03"])
    clientes_cohorte: int
    recompra_3m: int
    recompra_6m: int
    recompra_12m: int
    gmv_acumulado: float
    gmv_primera_compra: float


class CohortesResponse(BaseModel):
    data: list[Cohorte]
    meta: Meta


class Satisfaccion(BaseModel):
    year_month: str = Field(examples=["2018-02"])
    tramo_retraso: Tramo
    reviews: int
    reviews_negativas: int
    reviews_positivas: int
    score_medio: float = Field(description="Promedio de 1 a 5")
    tiempo_respuesta_medio_h: float | None
    retraso_medio_dias: float | None = Field(
        description="Días respecto a la fecha prometida. Negativo = llegó antes de tiempo"
    )


class SatisfaccionResponse(BaseModel):
    data: list[Satisfaccion]
    meta: Meta
