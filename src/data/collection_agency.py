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
            "fihabilitadointerf",
            "fnimportepagare",
            "fcnombrerazonsocial",
            "fccodpostal",
            "fctelefono1",
            "fdfechaalta", #ms
            "fdfechabaja", #ms
            "fdcontrato",
        )
    )

    df_final = (
        tasclsegmendesp
        .join(tascldespacho, on="fidespid", how="left")

        # FECHAS BASE
        .withColumn(
            "fecha_alta",
            F.to_timestamp(F.from_unixtime(F.col("fdfechaalta") / 1000))
        )
        .withColumn(
            "fecha_baja",
            F.to_timestamp(F.from_unixtime(F.col("fdfechabaja") / 1000))
        )
        .withColumn(
            "fecha_contrato",
            F.to_timestamp(F.from_unixtime(F.col("fdcontrato") / 1000))
        )

        # FLAGS DE ESTADO / ACTIVIDAD
        .withColumn(
            "has_empresas_cobext",
            F.when(
                F.col("fcempresascobext").isNotNull() &
                (F.length(F.trim(F.col("fcempresascobext"))) > 0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "has_phone",
            F.when(
                F.col("fctelefono1").isNotNull() &
                (F.length(F.trim(F.col("fctelefono1"))) > 0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "has_cp",
            F.when(
                F.col("fccodpostal").isNotNull() &
                (F.length(F.trim(F.col("fccodpostal"))) > 0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "has_razon_social",
            F.when(
                F.col("fcnombrerazonsocial").isNotNull() &
                (F.length(F.trim(F.col("fcnombrerazonsocial"))) > 0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        # ANTIGÜEDAD / RECENCIA
        .withColumn(
            "antiguedad_alta_dias",
            F.when(
                F.col("fecha_alta").isNotNull(),
                F.datediff(F.current_date(), F.to_date(F.col("fecha_alta")))
            )
        )
        .withColumn(
            "antiguedad_contrato_dias",
            F.when(
                F.col("fecha_contrato").isNotNull(),
                F.datediff(F.current_date(), F.to_date(F.col("fecha_contrato")))
            )
        )

        # DESCOMPOSICIÓN DE FECHAS
        .withColumn("alta_year", F.year("fecha_alta"))
        .withColumn("alta_month", F.month("fecha_alta"))
        .withColumn("alta_quarter", F.quarter("fecha_alta"))
        .withColumn("contrato_year", F.year("fecha_contrato"))
        .withColumn("contrato_month", F.month("fecha_contrato"))
        .withColumn("contrato_quarter", F.quarter("fecha_contrato"))

        # CAPACIDAD / CARGA
        .withColumn(
            "clientes_x_gestor",
            F.when(
                F.col("fncapacidadideal") > 0,
                F.col("fnclientesactual") / F.col("fncapacidadideal")
            )
        )
        .withColumn(
            "capacidad_min_sobre_ideal",
            F.when(
                F.col("fncapacidadideal") > 0,
                F.col("fncapacidadmin") / F.col("fncapacidadideal")
            )
        )
        .withColumn(
            "capacidad_min_sobre_max",
            F.when(
                F.col("fncapacidadmax") > 0,
                F.col("fncapacidadmin") / F.col("fncapacidadmax")
            )
        )
        .withColumn(
            "rango_capacidad",
            F.col("fncapacidadmax") - F.col("fncapacidadmin")
        )
        .withColumn(
            "rango_capacidad_sobre_ideal",
            F.when(
                F.col("fncapacidadideal") > 0,
                (F.col("fncapacidadmax") - F.col("fncapacidadmin")) / F.col("fncapacidadideal")
            )
        )
        .withColumn(
            "rango_capacidad_sobre_max",
            F.when(
                F.col("fncapacidadmax") > 0,
                (F.col("fncapacidadmax") - F.col("fncapacidadmin")) / F.col("fncapacidadmax")
            )
        )

        # IMPORTE PAGARÉ
        .withColumn(
            "importe_por_cliente",
            F.when(
                F.col("fnclientesactual") > 0,
                F.col("fnimportepagare").cast("double") / F.col("fnclientesactual")
            )
        )
        .withColumn(
            "importe_por_capacidad_ideal",
            F.when(
                F.col("fncapacidadideal") > 0,
                F.col("fnimportepagare").cast("double") / F.col("fncapacidadideal")
            )
        )
        .withColumn(
            "importe_por_capacidad_max",
            F.when(
                F.col("fncapacidadmax") > 0,
                F.col("fnimportepagare").cast("double") / F.col("fncapacidadmax")
            )
        )

        # STRINGS: LONGITUD / LIMPIEZA LIGERA
        .withColumn(
            "telefono_digits",
            F.regexp_replace(F.col("fctelefono1"), "[^0-9]", "")
        )
        .withColumn(
            "telefono_digits_len",
            F.length(F.col("telefono_digits"))
        )
        .withColumn(
            "telefono_valido_10_12_flag",
            F.when(
                F.col("telefono_digits").isNotNull() &
                F.col("telefono_digits_len").between(10, 12),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "cp_5digitos_flag",
            F.when(
                F.col("fccodpostal").rlike("^[0-9]{5}$"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        # FEATURES COMBINADAS INTERESANTES
        .withColumn(
            "clientes_x_antiguedad",
            F.when(
                F.col("antiguedad_alta_dias") > 0,
                F.col("fnclientesactual") / F.col("antiguedad_alta_dias")
            )
        )
        .withColumn(
            "importe_x_antiguedad",
            F.when(
                F.col("antiguedad_alta_dias") > 0,
                F.col("fnimportepagare").cast("double") / F.col("antiguedad_alta_dias")
            )
        )

        # =========================================================
        # SCORE SIMPLE DE COMPLETITUD
        # =========================================================
        .withColumn(
            "info_basica_score",
            (
                F.coalesce(F.col("has_phone"), F.lit(0)) +
                F.coalesce(F.col("has_cp"), F.lit(0)) +
                F.coalesce(F.col("has_razon_social"), F.lit(0)) +
                F.coalesce(F.col("telefono_valido_10_12_flag"), F.lit(0)) +
                F.coalesce(F.col("cp_5digitos_flag"), F.lit(0))
            )
        )
        #.where(F.col("estatus") == 1)
        .select(
            "fidespid",
            "fitipodepto",
            "fncapacidadideal",
            "fncapacidadmin",
            "fncapacidadmax",
            "fnclientesactual",
            "fntipodespacho",
            "fitipogestion",
            "fitiempomanejocobza",
            "fihabilitadointerf",
            "fnimportepagare",
            "has_empresas_cobext",
            "has_phone",
            "has_cp",
            "has_razon_social",
            "antiguedad_alta_dias",
            "clientes_x_gestor",
            "capacidad_min_sobre_ideal",
            "capacidad_min_sobre_max",
            "rango_capacidad",
            "rango_capacidad_sobre_ideal",
            "rango_capacidad_sobre_max",
            "importe_por_cliente",
            "importe_por_capacidad_ideal",
            "importe_por_capacidad_max",
            "telefono_valido_10_12_flag", 
            "cp_5digitos_flag",
            "clientes_x_antiguedad", 
            "importe_x_antiguedad",
            "info_basica_score"
        )
    )

    return df_final