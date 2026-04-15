# Necessary imports.
import os
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    Window,
)
from typing import List

def build_agency_history(
    spark: SparkSession,
    week: int,
    start_week: int,
    segm_legal: List[int]
) -> DataFrame:
    
    threphonorarios = (
        spark.table("rd_baz_bdclientes.rd_threphonorarios")
        .withColumn(
            "week",
            F.concat(
                F.col("fianio"),
                F.lpad(F.col("fisemana"), 2, "0")
            ).cast("int")
        )
        .where((F.col("week") >= week))
    )

    threphonorarios = (
        spark.table("rd_baz_bdclientes.rd_threphonorarios")
        .where((F.col("fitipodepto").isin(segm_legal)))
        .select(
            
            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).alias("client_id"),
            "fitipodepto",
            "fdfechabono",
            "fideptoid",
            "fidespid",
            "fidiasatraso",
            "fnrecupcapital",
            "fnrecupmoratorios",
            "fiporcentajecomision",
            "fccomisiontotalcliente",
            "fcplan",
            "fcconceptocalculo",
            "fdfechagestion",
            "fdfecharegistrogestion",
            "fncargoautomatico",
            "fctipocartera",
            "fccampania",
            "fisematrasomax",
            "fnmontocargoautomatico",
            "fnmontocaptacion",
            "fnporcentajeefi",
            "fnporcentajemeta",
            "fiestrategia",
        )
    )
"""
threphonorarios = (
    spark.table("rd_baz_bdclientes.rd_threphonorarios")
    .select(
        "fianio",
        "fisemana",
        "fipais",
        "ficanal", # key
        "fisucursal",
        "fifolio",
        "fideptoid", # gerencia
        "fidespid",
        "fitipodepto", # key: tipo segmento
        "fnrecupcapital",
        "fnrecupmoratorios",
        "fiporcentajecomision",
        "fccomisiontotalcliente",
        "fcplan",
        "fdfechagestion",
        "fctipocartera",
        "fcproducto",
        "fccampania",
        "figestor",
        "fcempnum",
        "fisematrasomax",
        "fiestrategia",
    )
)

tacuabonado = (
    spark.table("rd_baz_bdclientes.rd_tacuabonado")
    .select(
        "fianio",
        "fisemana",
        "fipais",
        "ficanal", # key
        "fisucursal",
        "fifolio",
        "fdfechabono",
        "fisematrasomax",
        "fitipodepto", # key: tipo segmento
        "fideptoid", # gerencia
        "fidespid",
        "ficlasifcuenta",
        "fidiasatrasoabono",
        "fnmontoabono",
        "fncargoautomatico",
        "fitipocartera",
        "fiindjudextraju",
        "fiorigencartera",
        "fnsaldocte",
        "fnsaldoatrasadocte",
        "fnmoratorioscte",
        "figestor",
    )
)

tahonorariosdes = (
    spark.table("rd_baz_bdclientes.rd_tahonorariosdes")
    .select(
        "fianio",
        "fisemana",
        "fitipodepto", # key: tipo de segmento
        "fncomision",
        "fnporcentajecom",
        "fngestionesinv", # id numero de gestiones
        "fncobdespacho", # cobro realizado
        "fniva",
        "fnsubtotal",
        "fnretencioniva",
        "fnretencionisr",
        "fntotalapagar",
        "fcfopresupuesto",
        "fiiddespacho",
    )
)

taresultgestor = (
    spark.table("rd_baz_bdclientes.rd_taresultgestor")
    .select(
        "fianio",
        "fisemana",
        "fitipodepto",
        "fideptoid",
        "fidespid",
        "figestor",
        "fitipocartera",
        "fitotalclientes",
        "fnsaldo",
        "fnmoratorios",
        "fnsaldoatrasado",
        "fncobranzaefectivo",
        "fncobranzarmd",
        "fncargosaut",
        "fntotal",
        "finumabonosefectivo",
        "finumcargosaut",
        "fnmontocargoautomatico",
        "fnmontocaptacion",
    )
)
"""