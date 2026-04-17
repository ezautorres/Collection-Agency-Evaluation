"""
Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
import os
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    Window,
)
from typing import List














## FECHAS BASE
        #.withColumn(
        #    "fecha_alta",
        #    F.to_timestamp(F.from_unixtime(F.col("fdfechaalta") / 1000))
        #)
        #.withColumn(
        #    "fecha_baja",
        #    F.to_timestamp(F.from_unixtime(F.col("fdfechabaja") / 1000))
        #)
#
        ## FLAGS DE ESTADO / ACTIVIDAD
        #.withColumn(
        #    "has_empresas_cobext",
        #    F.when(
        #        F.col("fcempresascobext").isNotNull() &
        #        (F.length(F.trim(F.col("fcempresascobext"))) > 0),
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "has_phone",
        #    F.when(
        #        F.col("fctelefono1").isNotNull() &
        #        (F.length(F.trim(F.col("fctelefono1"))) > 0),
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "has_cp",
        #    F.when(
        #        F.col("fccodpostal").isNotNull() &
        #        (F.length(F.trim(F.col("fccodpostal"))) > 0),
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "has_razon_social",
        #    F.when(
        #        F.col("fcnombrerazonsocial").isNotNull() &
        #        (F.length(F.trim(F.col("fcnombrerazonsocial"))) > 0),
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
#
        ## ANTIGÜEDAD / RECENCIA
        #.withColumn(
        #    "antiguedad_alta_dias",
        #    F.when(
        #        F.col("fecha_alta").isNotNull(),
        #        F.datediff(F.current_date(), F.to_date(F.col("fecha_alta")))
        #    )
        #)
#
        ## CAPACIDAD / CARGA
        #.withColumn(
        #    "clientes_x_gestor",
        #    F.when(
        #        F.col("fncapacidadideal") > 0,
        #        F.col("fnclientesactual") / F.col("fncapacidadideal")
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "capacidad_min_sobre_ideal",
        #    F.when(
        #        F.col("fncapacidadideal") > 0,
        #        F.col("fncapacidadmin") / F.col("fncapacidadideal")
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "capacidad_min_sobre_max",
        #    F.when(
        #        F.col("fncapacidadmax") > 0,
        #        F.col("fncapacidadmin") / F.col("fncapacidadmax")
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "rango_capacidad",
        #    F.col("fncapacidadmax") - F.col("fncapacidadmin")
        #)
        #.withColumn(
        #    "rango_capacidad_sobre_ideal",
        #    F.when(
        #        F.col("fncapacidadideal") > 0,
        #        (F.col("fncapacidadmax") - F.col("fncapacidadmin")) / F.col("fncapacidadideal")
        #    )
        #)
        #.withColumn(
        #    "rango_capacidad_sobre_max",
        #    F.when(
        #        F.col("fncapacidadmax") > 0,
        #        (F.col("fncapacidadmax") - F.col("fncapacidadmin")) / F.col("fncapacidadmax")
        #    )
        #)
#
        ## STRINGS: LONGITUD / LIMPIEZA LIGERA
        #.withColumn(
        #    "telefono_digits",
        #    F.regexp_replace(F.col("fctelefono1"), "[^0-9]", "")
        #)
        #.withColumn(
        #    "telefono_digits_len",
        #    F.length(F.col("telefono_digits"))
        #)
        #.withColumn(
        #    "telefono_valido_10_12_flag",
        #    F.when(
        #        F.col("telefono_digits").isNotNull() &
        #        F.col("telefono_digits_len").between(10, 12),
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
        #.withColumn(
        #    "cp_5digitos_flag",
        #    F.when(
        #        F.col("fccodpostal").rlike("^[0-9]{5}$"),
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
#
        ## FEATURES COMBINADAS INTERESANTES
        #.withColumn(
        #    "clientes_x_antiguedad",
        #    F.when(
        #        F.col("antiguedad_alta_dias") > 0,
        #        F.col("fnclientesactual") / F.col("antiguedad_alta_dias")
        #    )
        #)
#
        ## =========================================================
        ## SCORE SIMPLE DE COMPLETITUD
        ## =========================================================
        #.withColumn(
        #    "info_basica_score",
        #    (
        #        F.coalesce(F.col("has_phone"), F.lit(0)) +
        #        F.coalesce(F.col("has_cp"), F.lit(0)) +
        #        F.coalesce(F.col("has_razon_social"), F.lit(0)) +
        #        F.coalesce(F.col("telefono_valido_10_12_flag"), F.lit(0)) +
        #        F.coalesce(F.col("cp_5digitos_flag"), F.lit(0))
        #    )
        #)
        #.select(
        #    "fidespid",
        #    "fitipodepto",
        #    "estatus",
        #    "fncapacidadideal",
        #    "fncapacidadmin",
        #    "fncapacidadmax",
        #    "fnclientesactual",
        #    "fntipodespacho",
        #    "fitipogestion",
        #    "fitiempomanejocobza",
        #    "fihabilitadointerf",
        #    "has_empresas_cobext",
        #    "has_phone",
        #    "has_cp",
        #    "has_razon_social",
        #    "antiguedad_alta_dias",
        #    "clientes_x_gestor",
        #    "capacidad_min_sobre_ideal",
        #    "capacidad_min_sobre_max",
        #    "rango_capacidad",
        #    "rango_capacidad_sobre_ideal",
        #    "rango_capacidad_sobre_max",
        #    "telefono_valido_10_12_flag", 
        #    "cp_5digitos_flag",
        #    "clientes_x_antiguedad", 
        #    "info_basica_score"
        #)