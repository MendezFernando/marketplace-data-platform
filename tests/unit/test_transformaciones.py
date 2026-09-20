"""Pruebas de las funciones de transformación compartidas.

Estas funciones devuelven una `Column`, que no es un valor sino una INSTRUCCIÓN
para Spark. Por eso cada prueba crea un DataFrame diminuto, aplica la columna y
comprueba el resultado real.

Varias de estas pruebas son de REGRESIÓN: blindan errores que ya costó encontrar
una vez (metadatos colándose, claves nulas que hacían desaparecer filas al unir).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from marketplace_dp.common.facts import date_key, resolve_key
from marketplace_dp.common.lakehouse import (
    bronze_path,
    clean_code,
    clean_text,
    to_money,
    to_timestamp,
)


def evaluar(spark, valor, columna_fn, tipo="string"):
    """Aplica una expresión de columna a un único valor y devuelve el resultado."""
    df = spark.createDataFrame([(valor,)], f"valor {tipo}")
    return df.select(columna_fn("valor").alias("resultado")).first()["resultado"]


# ─────────────────────────────────────────────────────────────────────────────
#  clean_text — texto libre
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "entrada, esperado",
    [
        ("  Hola  Mundo  ", "hola mundo"),  # recorta y colapsa espacios internos
        ("SAO PAULO", "sao paulo"),  # baja a minúsculas
        ("ya normalizado", "ya normalizado"),  # idempotente: no rompe lo correcto
        ("", None),  # cadena vacía = ausencia de valor
        ("   ", None),  # solo espacios, idem
        (None, None),  # el nulo se propaga
    ],
)
def test_clean_text(spark, entrada, esperado):
    assert evaluar(spark, entrada, clean_text) == esperado


# ─────────────────────────────────────────────────────────────────────────────
#  clean_code — códigos cortos (estados, siglas)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "entrada, esperado",
    [
        (" sp ", "SP"),  # los códigos van en MAYÚSCULAS, al revés que el texto
        ("rj", "RJ"),
        ("", None),
        (None, None),
    ],
)
def test_clean_code(spark, entrada, esperado):
    assert evaluar(spark, entrada, clean_code) == esperado


def test_texto_y_codigo_normalizan_en_direcciones_opuestas(spark):
    """Regresión: `clean_text` baja y `clean_code` sube. Intercambiarlas rompería
    los joins por estado, porque 'sp' y 'SP' no casan."""
    assert evaluar(spark, "SP", clean_text) == "sp"
    assert evaluar(spark, "sp", clean_code) == "SP"


# ─────────────────────────────────────────────────────────────────────────────
#  to_money — importes
# ─────────────────────────────────────────────────────────────────────────────


def test_to_money_devuelve_decimal_no_float(spark):
    """El tipo importa: FLOAT acumula descuadres de céntimos sobre millones de filas."""
    resultado = evaluar(spark, "12.50", to_money)
    assert isinstance(resultado, Decimal)
    assert resultado == Decimal("12.50")


def test_to_money_redondea_a_dos_decimales(spark):
    assert evaluar(spark, "10.999", to_money) == Decimal("11.00")


def test_to_money_no_pierde_precision_al_sumar(spark):
    """0.1 + 0.2 en coma flotante NO da 0.3. En DECIMAL sí."""
    df = spark.createDataFrame([("0.10",), ("0.20",)], "valor string")
    total = df.agg({"valor": "sum"}).first()[0]  # suma como double: 0.30000000000000004
    total_decimal = df.select(to_money("valor").alias("v")).agg({"v": "sum"}).first()[0]
    assert total_decimal == Decimal("0.30")
    assert float(total) == pytest.approx(0.3)


# ─────────────────────────────────────────────────────────────────────────────
#  to_timestamp — fechas del origen
# ─────────────────────────────────────────────────────────────────────────────


def test_to_timestamp_parsea_el_formato_del_origen(spark):
    resultado = evaluar(spark, "2018-09-04 10:30:00", to_timestamp)
    assert resultado.year == 2018 and resultado.month == 9 and resultado.day == 4


def test_to_timestamp_devuelve_null_en_vez_de_fallar(spark):
    """Una orden sin entregar no tiene fecha de entrega: eso es un hecho de
    negocio, no un error. El pipeline no debe caerse por ello."""
    assert evaluar(spark, "no es una fecha", to_timestamp) is None


# ─────────────────────────────────────────────────────────────────────────────
#  date_key — clave sustituta de dim_date
# ─────────────────────────────────────────────────────────────────────────────


def test_date_key_genera_formato_yyyymmdd(spark):
    df = spark.createDataFrame([("2018-09-04 10:30:00",)], "fecha string")
    resultado = df.select(date_key("fecha").alias("sk")).first()["sk"]
    assert resultado == 20180904


def test_date_key_resuelve_nulo_al_miembro_desconocido(spark):
    """REGRESIÓN: con NULL, la fila entera desaparecería al unirse con dim_date.
    Si alguien quita el coalesce, esta prueba falla."""
    df = spark.createDataFrame([(None,)], "fecha string")
    assert df.select(date_key("fecha").alias("sk")).first()["sk"] == -1


# ─────────────────────────────────────────────────────────────────────────────
#  resolve_key — claves sustitutas de las dimensiones
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("entrada, esperado", [("abc123", "abc123"), (None, "-1")])
def test_resolve_key(spark, entrada, esperado):
    """REGRESIÓN del bug del 45% de GMV sin vendedor: una clave sin resolver debe
    quedar visible como 'Unknown', nunca evaporarse en el siguiente join."""
    assert evaluar(spark, entrada, resolve_key) == esperado


# ─────────────────────────────────────────────────────────────────────────────
#  bronze_path — construcción de rutas (sin Spark: función pura de verdad)
# ─────────────────────────────────────────────────────────────────────────────


def test_bronze_path_incluye_tabla_y_particion():
    ruta = bronze_path("olist_orders_dataset", date(2018, 9, 4))
    assert ruta.startswith("s3a://")
    assert "olist_orders_dataset" in ruta
    assert "ingestion_date=2018-09-04" in ruta


def test_bronze_path_usa_formato_iso_en_la_particion():
    """REGRESIÓN: si la fecha se formatea distinto (04-09-2018), Silver no
    encontraría la partición y el pipeline fallaría solo al reprocesar."""
    assert "ingestion_date=2018-01-05" in bronze_path("t", date(2018, 1, 5))
