"""
collection.py
-------------

Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
)
from pyspark.sql.window import Window
from typing import List


def create_historical_base(
    spark: SparkSession,
    start_week_: int,
    segm_legal: List[int],
) -> DataFrame:
    
    df_clients = _get_clients_pivot(spark, start_week_, segm_legal)
    cteu = _extract_clasificacteu_info(spark, df_clients, start_week_, segm_legal)
    threp = _extract_threphonorarios_info(spark, df_clients, start_week_)
    
    df = (
        cteu
        .join(threp, on=["client_id", "fisemana"], how="left")
        .where(F.col("fidespid").isNotNull())
        .withColumn(
            "deuda_saldada",
            F.when(
                (F.col("in_legal") == 1) &
                (F.col("cobranza") >= F.col("fnsaldocte")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "lost_track",
            F.when(
                (F.col("in_legal") == 1) &
                (F.col("with_next_fisematrasomax") == 0) &
                (F.col("with_next_fitipodepto") == 0) &
                (F.col("deuda_saldada") == 0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    return df

def _get_clients_pivot(
    spark: SparkSession,
    start_week_: int,
    segm_legal: List[int],
) -> DataFrame:
    """
    Extract pivot table of clients.
    """
    return (
        spark.table("rd_baz_bdclientes.rd_clasificacteu")
        .where(
            (F.col("fisemana") > F.lit(start_week_)) &
            (F.col("fitipodepto").isin(segm_legal))
        )
        .select(
            # ID Client.
            F.concat_ws(    
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).cast("string").alias("client_id"),
        )
        .where(F.col("client_id").isNotNull())
        .dropDuplicates(["client_id"])
    )


def _extract_clasificacteu_info(
    spark: SparkSession,
    df_filter: DataFrame,
    start_week_: int,
    segm_legal: List[int],
) -> DataFrame:
    """
    Extract classification information for clients.
    """
    # Window for selecting the last record per client and week.
    w_last_record = (
        Window
        .partitionBy("client_id", "fisemana")
        .orderBy(
            F.col("fdfechasurtimiento").desc(),
            F.col("fdfechaultimaact").desc(),
        )
    )

    # Windows for ordering weeks.
    w_prev = (
        Window
        .partitionBy("client_id")
        .orderBy(F.col("fisemana").asc())
    )
    w_next = (
        Window
        .partitionBy("client_id")
        .orderBy(F.col("fisemana").desc())
    )

    cteu = (
        spark.table("rd_baz_bdclientes.rd_clasificacteu")
        .where(F.col("fisemana") >= F.lit(start_week_))
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).alias("client_id"),

            F.col("fisemana"),
            F.col("fitipodepto"),
            F.col("fisematrasomax"),
            F.col("fnsaldocte"),
            F.col("fnultimoabono"),
            F.col("fnultabonomora"),
            F.col("fdfechasurtimiento"),
            F.col("fdfechaultimaact"),

            F.col("finogestiones"),
            F.col("finogestsincobro"),
        )
        .where(
            F.col("client_id").isNotNull() &
            F.col("fisemana").isNotNull()
        )
        .join(df_filter, on="client_id", how="inner")

        # Select the last record per client and week.
        .withColumn("rn", F.row_number().over(w_last_record))
        .where(F.col("rn") == 1)
        .drop("rn", "fdfechasurtimiento", "fdfechaultimaact")
        
        # Required pay.
        .withColumn(
            "pago_requerido",
            F.lag(
                F.coalesce(F.col("fnultimoabono"), F.lit(0)) +
                F.coalesce(F.col("fnultabonomora"), F.lit(0)),
                1
            ).over(w_prev)
        )
        .drop("fnultimoabono", "fnultabonomora")

        # Roll Forward.
        .withColumn(
            "prev_fisematrasomax",
            F.lag("fisematrasomax", 1).over(w_prev)
        )
        .withColumn(
            "roll_forward",
            F.when(
                (F.col("prev_fisematrasomax").isNotNull()) &
                (F.col("fisematrasomax") == F.col("prev_fisematrasomax") + 1),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        # Roll Backward.
        .withColumn(
            "next_fisematrasomax",
            F.lag("fisematrasomax", 1).over(w_next)
        )
        .withColumn(
            "roll_backward",
            F.when(
                (F.col("next_fisematrasomax").isNotNull()) &
                (F.col("fisematrasomax") == F.col("next_fisematrasomax") + 1),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "with_prev_fisematrasomax",
            F.col("prev_fisematrasomax").isNotNull().cast("int")
        )
        .withColumn(
            "with_next_fisematrasomax",
            F.col("next_fisematrasomax").isNotNull().cast("int")
        )

        # Segms movement.
        .withColumn("prev_fitipodepto", F.lag("fitipodepto", 1).over(w_prev))
        .withColumn("next_fitipodepto", F.lag("fitipodepto", 1).over(w_next))
        .withColumn(
            "enter_legal",
            F.when(
                (F.col("fitipodepto").isin(segm_legal)) &
                (F.col("prev_fitipodepto").isin(segm_legal) == False),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "out_legal",
            F.when(
                (F.col("fitipodepto").isin(segm_legal)) &
                (F.col("next_fitipodepto").isin(segm_legal) == False),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "in_legal",
            F.when(
                (F.col("fitipodepto").isin(segm_legal)),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "with_prev_fitipodepto",
            F.col("prev_fitipodepto").isNotNull().cast("int")
        )
        .withColumn(
            "with_next_fitipodepto",
            F.col("next_fitipodepto").isNotNull().cast("int")
        )
    )

    return cteu

def _extract_threphonorarios_info(
    spark: SparkSession,
    df_filter: DataFrame,
    start_week_: int,
) -> DataFrame:
    """
    """
    # Window for selecting the last record per client and week.
    w_last_record = (
        Window
        .partitionBy(["client_id", "fisemana"])
        .orderBy(F.col("fdfechagestion").desc())
    )

    threphonorarios = (
        spark.table("rd_baz_bdclientes.rd_threphonorarios")
        .withColumn(
            "fisemana",
            F.concat(
                F.col("fianio"),
                F.lpad(F.col("fisemana"), 2, "0")
            ).cast("int")
        )
        .where((F.col("fisemana") >= start_week_))
        .select(
            
            # Client ID.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).alias("client_id"),

            F.col("fidespid"),
            F.col("fisemana"),
            
            F.col("fnrecupcapital"),
            F.col("fnrecupmoratorios"),
            F.col("fnmontocargoautomatico"),
            F.col("fnmontocaptacion"),
            F.col("figestor"),

            F.col("fcplan"),
            F.col("fdfechagestion"),

        )
        .where(
            F.col("client_id").isNotNull() &
            F.col("fidespid").isNotNull() &
            F.col("fisemana").isNotNull()
        )
        .join(df_filter, on="client_id", how="inner")
        .withColumn(
            "actived_plan",
            F.when(
                F.col("fcplan") == "PLAN ACTIVO",
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    last_week_info = (
        threphonorarios
        .withColumn("rn", F.row_number().over(w_last_record))
        .where(F.col("rn") == 1)
        .select(
            F.col("client_id"),
            F.col("fidespid"),
            F.col("fisemana"),
            F.col("actived_plan")
        )
    )

    cum_info = (
        threphonorarios
        .groupBy(["client_id", "fisemana"])
        .agg(
            #(
            #    F.sum(F.coalesce(F.col("fnrecupcapital"), F.lit(0))) +
            #    F.sum(F.coalesce(F.col("fnrecupmoratorios"), F.lit(0)))
            #).alias("cobranza"),
            F.sum(
                F.when(
                    (F.col("fnrecupcapital") + F.col("fnrecupmoratorios")) > 0,
                    F.col("fnrecupcapital") + F.col("fnrecupmoratorios"),
                ).otherwise(F.lit(0)),
            ).alias("cobranza"),
            F.sum("fnmontocargoautomatico").alias("fnmontocargoautomatico"),
            F.sum("fnmontocaptacion").alias("fnmontocaptacion"),
            F.countDistinct("figestor").alias("n_gestors"),
        )
        .withColumn(
            "payment_indicator",
            F.when(
                (F.col("cobranza")) > 0,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

    return last_week_info.join(cum_info, on=["client_id", "fisemana"], how="inner")











































def _extract_recup_cuotas_info(
    spark: SparkSession,
    df_filter: DataFrame,
    start_week_: int,
) -> DataFrame:
    """
    Extract payment information for clients.
    """
    recup = (
        spark.table("rd_baz_bdclientes.rd_recuperacion_cuotas_cobranza")
        .withColumn(
            "fisemana",
            F.concat(
                F.col("fianioproceso"),
                F.lpad(F.col("fisemproceso"), 2, "0"),
            ).cast("int"),
        )
        .where((F.col("fisemana") >= F.lit(start_week_)))
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipaiscu").cast("string"),
                F.col("ficanalcu").cast("string"),
                F.col("fisucursalcu").cast("string"),
                F.col("fifoliocu").cast("string"),
            ).alias("client_id"),

            F.col("fisemana"),
            #F.col("fdcreccapital"),
            #F.col("fdcrecmoratorio"),
            F.col("fitipocobranza"),
        )
        .where(
            F.col("client_id").isNotNull() &
            F.col("fisemana").isNotNull()
        )
        .join(df_filter, on="client_id", how="inner")
 
        # Grouped.
        .groupBy("client_id", "fisemana")
        .agg(
            #(
            #    F.sum(F.coalesce(F.col("fdcreccapital"), F.lit(0))) +
            #    F.sum(F.coalesce(F.col("fdcrecmoratorio"), F.lit(0)))
            #).alias("cobranza"),
            #F.sum(
            #    F.when(
            #        (F.col("fdcreccapital") + F.col("fdcrecmoratorio")) > 0,
            #        F.col("fdcreccapital") + F.col("fdcrecmoratorio"),
            #    ).otherwise(F.lit(0)),
            #).alias("cobranza"),
            (
                F.countDistinct("fitipocobranza") > 1
            ).cast("int").alias("multi_gestion"),
        )
        #.withColumn(
        #    "payment_indicator",
        #    F.when(
        #        (F.col("cobranza")) > 0,
        #        F.lit(1)
        #    ).otherwise(F.lit(0))
        #)
    )

    return recup


def _extract_taresultgestionhist(
    spark: SparkSession,
    df_filter: DataFrame,
    start_week_: int
) -> DataFrame:
    """
    Extract information about client visits and gestion.
    """
    df = (
        _add_week_column(
            spark,
            spark.table("rd_baz_bdclientes.rd_taresultgestionhist")
        )
        .withColumnRenamed("yearweek", "fisemana")
        .where(
            (F.col("fisemana") >= F.lit(start_week_)) &
            (F.col("fiaccionges") >= F.lit(800))
        )
        .select(
            
            # Client ID.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).alias("client_id"),

            F.col("fisemana"),
            F.col("figestor"),
            F.col("fiaccionges")
        )
        .join(df_filter, on="client_id", how="inner")
        .groupBy("client_id", "fisemana")
        .agg(
            F.count(F.lit(1)).alias("n_gestions_hist"),
            F.countDistinct("figestor").alias("n_gestors_hist"),
        )
    )
    return df

def _extract_tacob_info(
    spark: SparkSession,
    df_filter: DataFrame,
    start_week_: int,
    contact_codes: List[int],
) -> DataFrame:
    """
    Extract information about client visits and gestion.
    """
    tacob_raw = (
        spark.table("rd_baz_bdclientes.rd_tacobgpshh")
        .where(
            (F.col("fisemana") >= F.lit(start_week_)) &
            (F.col("fivisitaid") >= 800)
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipaiscu").cast("string"),
                F.col("ficanalcu").cast("string"),
                F.col("fisucursalcu").cast("string"),
                F.col("fifoliocu").cast("string"),
            ).alias("client_id"),

            F.col("fisemana"),
        )
        .where(
            F.col("client_id").isNotNull() &
            F.col("fisemana").isNotNull()
        )
        .join(df_filter, on="client_id", how="inner")
    )

    return (
        tacob_raw
        .groupBy("client_id", "fisemana")
        .agg(F.count(F.lit(1)).alias("n_gestions"))
    )



def _add_week_column(spark: SparkSession, df: DataFrame) -> DataFrame:
    dates = (
        spark.table("rd_baz_bdclientes.rd_fechas_semanas")
        .withColumn(
            "yearweek",
            F.concat(
                F.col("fianioproceso"),
                F.lpad(F.col("fisemproceso"), 2, "0")
            ).cast("int")
        )
        .withColumn(
            "start_date",
            (F.col("fcfecinicial") / 1000).cast("timestamp").cast("date")
        )
        .withColumn(
            "end_date",
            (F.col("fcfectermino") / 1000).cast("timestamp").cast("date")
        )
        .select("yearweek", "start_date", "end_date")
    )

    return (
        df
        .withColumn(
            "fifecha_date",
            F.to_date(F.col("fifecha").cast("string"), "yyyyMMdd")
        )
        .join(
            dates,
            (df["fifecha_date"] >= dates["start_date"]) &
            (df["fifecha_date"] <= dates["end_date"]),
            how="left"
        )
        .drop("start_date", "end_date", "fifecha_date")
    )