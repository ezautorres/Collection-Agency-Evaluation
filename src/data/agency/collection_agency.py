"""
collection_agency.py
--------------------

Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
from typing import List
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
)

def create_agency_base(
    spark: SparkSession,
    segm_legal: List[int],
) -> DataFrame:
    """
    Retrieve the agencies dataset for the specified parameters.
    """
    tasclsegmendesp = _extract_tasclsegmendesp_info(spark, segm_legal)
    tascldespacho = _extract_tascldespacho_info(spark)

    return tasclsegmendesp.join(tascldespacho, on="fidespid", how="left")


def _extract_tasclsegmendesp_info(
    spark: SparkSession,
    segm_legal: List[int],
) -> DataFrame:
    """
    Retrieve the agencies dataset for the specified segments.
    """
    return (
        spark.table("rd_baz_bdclientes.rd_tasclsegmendesp")
        .where(F.col("fitipodepto").isin(segm_legal))
        .select(
            F.col("fidespid"),
            F.col("fitipodepto"),
            F.col("estatus"),
            F.col("fncapacidadideal").alias("ideal_cap_manager"),
            F.col("fncapacidadmin").alias("min_cap_manager"),
            F.col("fncapacidadmax").alias("max_cap_manager"),
            F.col("fnclientesactual").alias("current_clients")
        )
    )


def _extract_tascldespacho_info(spark: SparkSession) -> DataFrame:
    """
    Add agencies information to the dataset.
    """
    table_name = "rd_baz_bdclientes.rd_tascldespacho"
    return (
        spark.table(table_name)
        .select(
            F.col("fidespid"),
            F.col("fntipodespacho"),
            F.col("fitipogestion"),
            F.col("fitiempomanejocobza"),
            F.col("fcnombrerazonsocial"),
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fcrfc as string),"
                f"'{table_name}',"
                "'fcrfc', 1, ''"
                ")"
            ).alias("fcrfc"),
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fcemail1 as string),"
                f"'{table_name}',"
                "'fcemail1', 1, ''"
                ")"
            ).alias("fcemail"),
            F.col("fchomoclave"),
            F.col("fiidtipopersfiscal"),
            F.col("fccodpostal"),
            F.col("fctelefono1"),
            F.col("fcempresascobext"),
            F.col("fdfechaalta"),
            F.col("fdfechabaja"),
            F.col("fihabilitadointerf"),
        )
    )