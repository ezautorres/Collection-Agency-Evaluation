# Necessary imports.
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
)
from typing import List

def create_agency_base(
    spark: SparkSession,
    segm_legal: List[int]
) -> DataFrame:
    """
    Retrieve the despachos dataset for the specified parameters.
    """
    tasclsegmendesp = (
        spark.table("rd_baz_bdclientes.rd_tasclsegmendesp")
        .where(F.col("fitipodepto").isin(segm_legal))
        .select(
            "fidespid", # key
            "fitipodepto", # key: tipo de segmento
            "estatus", # 0,1
            "fncapacidadideal", # de gestores
            "fncapacidadmin",
            "fncapacidadmax",
            "fnclientesactual",
        )
    )

    tascldespacho = (
        spark.table("rd_baz_bdclientes.rd_tascldespacho")
        .select(
            "fidespid", # key
            "fntipodespacho",
            "fitipogestion",
            "fitiempomanejocobza",
            "fcempresascobext", # empresas registradas al despacho
            "fcrfc",
            "fchomoclave",
            "fcnombrerazonsocial",
            "fihabilitadointerf",
            "fiidtipopersfiscal",
            "fccodpostal",
            "fctelefono1",
            "fdfechaalta", #ms
            "fdfechabaja", #ms
        )
    )

    out = (
        tasclsegmendesp
        .join(tascldespacho, on="fidespid", how="left")
    )

    return out