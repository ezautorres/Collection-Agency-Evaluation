from typing import List
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

UMBRAL_EFICIENCIA = 0.6


def create_features(
    raw: DataFrame,
    segm_legal: List[int],
) -> DataFrame:
    base = raw.where(F.col("fitipodepto").isin(segm_legal))
    
    metricas_globales = _get_ratios(base)
    metricas_globales = _add_cat(metricas_globales)
    
    metricas_semanales = _get_ratios(base, weekly=True)
    tendencias = _get_tendencias(metricas_semanales)


    # JOIN FINAL
    final = (
        metricas_globales
        .withColumn("avg_clientes", F.col("n_clientes") / 12)
        .join(tendencias, on=["fidespid", "fitipodepto"], how="left")
    )
    
    return final

def _add_cat(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "cat_eficiencia",
            F.when(
                F.col("eficiencia").isNull(),
                "Sin datos"
            ).when(
                F.col("eficiencia") >= UMBRAL_EFICIENCIA,
                "Alta eficiencia"
            ).when(
                F.col("eficiencia") >= UMBRAL_EFICIENCIA * 0.5,
                "Eficiencia media"
            ).otherwise("Baja eficiencia")
        )
        .withColumn(
            "cat_intensidad_gestion",
            F.when(
                F.col("pct_clientes_gestionados") >= 0.7,
                "Gestión intensa"
            ).when(
                F.col("pct_clientes_gestionados") >= 0.4,
                "Gestión moderada"
            ).otherwise("Gestión escasa")
        )
        .withColumn(
            "cat_gestion_efectiva",
            F.when(
                F.col("pct_clientes_pagaron") >= 0.7,
                "Gestión intensa"
            ).when(
                F.col("pct_clientes_pagaron") >= 0.4,
                "Gestión moderada"
            ).otherwise("Gestión escasa")
        )
    )

def _get_ratios(base: DataFrame, weekly: bool = False):

    out_cols = [
        "fidespid",
        "fitipodepto",
        "n_clientes",
        "cobranza",
        "pago_requerido",
        "eficiencia",
        "n_gestions",
        "pct_clientes_gestionados",
        "pct_clientes_pagaron",
        "pct_clientes_sin_deuda",
        "pct_clientes_lost_track",
        "pct_clientes_salieron",
        "pct_clientes_entraron",
        "pct_clientes_con_plan_activo", #! positivo?
        "saldo_x_cliente", #! cartera conflictiva?
        "gestiones_x_cliente",
        "pct_gestiones_efectivas",
        "pct_roll_forward",
        "pct_roll_backward",
        "avg_fisematrasomax",
    ]
    if weekly:
        grup = ["fidespid", "fitipodepto", "fisemana"]
        out_cols = out_cols + ["fisemana"]
    else:
        grup = ["fidespid", "fitipodepto"]
    return (
        base
        .groupBy(*grup)
        .agg(

            # Clientes
            F.countDistinct("client_id").alias("n_clientes"),
            F.countDistinct(
                F.when(F.col("finogestiones") > 0, F.col("client_id"))
            ).alias("n_clientes_con_gestiones"),
            F.sum("payment_indicator").alias("n_clientes_pagaron"),
            F.sum("deuda_saldada").alias("n_clientes_sin_deuda"),
            F.sum("lost_track").alias("n_clientes_lost_track"),
            F.sum("out_legal").alias("n_clientes_salidas"),
            F.sum("enter_legal").alias("n_clientes_entradas"),
            F.sum("actived_plan").alias("n_clientes_actived_plan"),

            # Cobranza.
            F.sum(F.col("cobranza")).alias("cobranza"),
            F.sum(F.col("pago_requerido")).alias("pago_requerido"),
            F.sum(F.col("fnsaldocte")).alias("fnsaldocte"),

            # Gestiones.
            F.sum(F.col("finogestiones")).alias("n_gestions"),
            F.sum(F.col("finogestsincobro")).alias("n_gestions_sin_cobro"),
            
            # Atraso semanas
            F.sum(
                F.when(
                    F.col("with_prev_fisematrasomax") == 1,
                    F.col("roll_forward")
                )
            ).alias("n_roll_forward"),
            F.sum("with_prev_fisematrasomax").alias("n_with_prev_fisematrasomax"),
            F.sum(
                F.when(
                    F.col("with_next_fisematrasomax") == 1,
                    F.col("roll_backward")
                )
            ).alias("n_roll_backward"),
            F.sum("with_next_fisematrasomax").alias("n_with_next_fisematrasomax"),
            F.avg("fisematrasomax").alias("avg_fisematrasomax"),
        )

        # Clientes
        .withColumn(
            "pct_clientes_gestionados",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_con_gestiones") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_clientes_pagaron",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_pagaron") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_clientes_sin_deuda",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_sin_deuda") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_clientes_lost_track",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_lost_track") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_clientes_salieron",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_salidas") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_clientes_entraron",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_entradas") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_clientes_con_plan_activo",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_clientes_actived_plan") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )

        # Cobranza
        .withColumn(
            "eficiencia",
            F.when(
                F.col("pago_requerido") > 0,
                F.col("cobranza") / F.col("pago_requerido")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "saldo_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("fnsaldocte") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )

        # Gestiones
        .withColumn(
            "gestiones_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_gestions") / F.col("n_clientes")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_gestiones_efectivas",
            F.when(
                F.col("n_gestions") > 0,
                (F.col("n_gestions") - F.col("n_gestions_sin_cobro")) / F.col("n_gestions")
            ).otherwise(F.lit(None))
        )

        # Atraso
        .withColumn(
            "pct_roll_forward",
            F.when(
                F.col("n_with_prev_fisematrasomax") > 0,
                F.col("n_roll_forward") / F.col("n_with_prev_fisematrasomax")
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "pct_roll_backward",
            F.when(
                F.col("n_with_next_fisematrasomax") > 0,
                F.col("n_roll_backward") / F.col("n_with_next_fisematrasomax")
            ).otherwise(F.lit(None))
        )
        .select(*out_cols)
    )


def _get_tendencias(df_weekly: DataFrame):
    w_ord = (
        Window
        .partitionBy("fidespid", "fitipodepto")
        .orderBy("fisemana")
    )
    df_weekly = df_weekly.withColumn(
        "t",
        F.row_number().over(w_ord).cast("double")
    )

    tendencias_raw = (
        df_weekly
        .groupBy("fidespid", "fitipodepto")
        .agg(
            F.count("fisemana").alias("n_semanas_obs"),

            _slope_col("eficiencia").alias("slope_eficiencia"),
            _slope_col("pct_clientes_gestionados").alias("slope_pct_clientes_gestionados"),
            _slope_col("pct_clientes_pagaron").alias("slope_pct_clientes_pagaron"),
            _slope_col("pct_clientes_sin_deuda").alias("slope_pct_clientes_sin_deuda"),
            _slope_col("pct_clientes_lost_track").alias("slope_pct_clientes_lost_track"),
            _slope_col("pct_clientes_salieron").alias("slope_pct_salidas"),
            _slope_col("pct_clientes_entraron").alias("slope_pct_entradas"),
            _slope_col("pct_clientes_con_plan_activo").alias("slope_pct_actived_plan"),
            _slope_col("saldo_x_cliente").alias("slope_saldo_x_cliente"),
            _slope_col("gestiones_x_cliente").alias("slope_pct_gestiones_x_cliente"),
            _slope_col("pct_gestiones_efectivas").alias("slope_pct_gestiones_sin_cobro"),
            _slope_col("pct_roll_forward").alias("slope_pct_roll_forward"),
            _slope_col("pct_roll_backward").alias("slope_pct_roll_backward"),
            _slope_col("avg_fisematrasomax").alias("slope_avg_fisematrasomax"),

            F.stddev("eficiencia").alias("std_eficiencia"),
        )
    )

    # PERCENTILES RELATIVOS AL UNIVERSO
    cortes = [0.33, 0.66]
    percentiles = (
        tendencias_raw
        .select(
            F.percentile_approx("slope_eficiencia",              cortes).alias("p_eficiencia"),
            F.percentile_approx("slope_pct_clientes_gestionados",cortes).alias("p_pct_clientes_gestionados"),
            F.percentile_approx("slope_pct_clientes_pagaron",    cortes).alias("p_pct_clientes_pagaron"),
            F.percentile_approx("slope_pct_clientes_sin_deuda",  cortes).alias("p_pct_clientes_sin_deuda"),
            F.percentile_approx("slope_pct_clientes_lost_track", cortes).alias("p_pct_clientes_lost_track"),
            F.percentile_approx("slope_pct_salidas",             cortes).alias("p_pct_salidas"),
            F.percentile_approx("slope_pct_entradas",            cortes).alias("p_pct_entradas"),
            F.percentile_approx("slope_pct_actived_plan",        cortes).alias("p_pct_actived_plan"),
            F.percentile_approx("slope_saldo_x_cliente",         cortes).alias("p_saldo_x_cliente"),
            F.percentile_approx("slope_pct_gestiones_x_cliente", cortes).alias("p_pct_gestiones_x_cliente"),
            F.percentile_approx("slope_pct_gestiones_sin_cobro", cortes).alias("p_pct_gestiones_sin_cobro"),
            F.percentile_approx("slope_pct_roll_forward",        cortes).alias("p_pct_roll_forward"),
            F.percentile_approx("slope_pct_roll_backward",       cortes).alias("p_pct_roll_backward"),
            F.percentile_approx("slope_avg_fisematrasomax",      cortes).alias("p_avg_fisematrasomax"),
            F.percentile_approx("std_eficiencia",                [0.5] ).alias("p_std_eficiencia"),
        )
        .collect()[0]
    )

    p33_eficiencia,               p66_eficiencia               = percentiles["p_eficiencia"]
    p33_pct_clientes_gestionados, p66_pct_clientes_gestionados = percentiles["p_pct_clientes_gestionados"]
    p33_pct_clientes_pagaron,     p66_pct_clientes_pagaron     = percentiles["p_pct_clientes_pagaron"]
    p33_pct_clientes_sin_deuda,   p66_pct_clientes_sin_deuda   = percentiles["p_pct_clientes_sin_deuda"]
    p33_pct_clientes_lost_track,  p66_pct_clientes_lost_track  = percentiles["p_pct_clientes_lost_track"]
    p33_pct_salidas,              p66_pct_salidas              = percentiles["p_pct_salidas"]
    p33_pct_entradas,             p66_pct_entradas             = percentiles["p_pct_entradas"]
    p33_pct_actived_plan,         p66_pct_actived_plan         = percentiles["p_pct_actived_plan"]
    p33_saldo_x_cliente,          p66_saldo_x_cliente          = percentiles["p_saldo_x_cliente"]
    p33_pct_gestiones_x_cliente,  p66_pct_gestiones_x_cliente  = percentiles["p_pct_gestiones_x_cliente"]
    p33_pct_gestiones_sin_cobro,  p66_pct_gestiones_sin_cobro  = percentiles["p_pct_gestiones_sin_cobro"]
    p33_pct_roll_forward,         p66_pct_roll_forward         = percentiles["p_pct_roll_forward"]
    p33_pct_roll_backward,        p66_pct_roll_backward        = percentiles["p_pct_roll_backward"]
    p33_avg_fisematrasomax,       p66_avg_fisematrasomax       = percentiles["p_avg_fisematrasomax"]
    p50_std_eficiencia                                         = percentiles["p_std_eficiencia"][0]

    return (
        tendencias_raw
        .withColumn(
            "tend_eficiencia",
            _con_minimo(
                _tendencia_3("slope_eficiencia", p33_eficiencia, p66_eficiencia)
            )
        )
        .withColumn(
            "tend_atraso",
            _con_minimo(
                _tendencia_3("slope_avg_fisematrasomax", p33_avg_fisematrasomax, p66_avg_fisematrasomax)
            )
        )
        .withColumn(
            "tend_gestions",
            _con_minimo(
                _tendencia_3("slope_pct_gestiones_x_cliente", p33_pct_gestiones_x_cliente, p66_pct_gestiones_x_cliente)
            )
        )
        .withColumn(
            "tend_gestions_sin_cobro",
            _con_minimo(
                _tendencia_3("slope_pct_gestiones_sin_cobro", p33_pct_gestiones_sin_cobro, p66_pct_gestiones_sin_cobro)
            )
        )
        .withColumn(
            "tend_pct_pagaron",
            _con_minimo(
                _tendencia_3("slope_pct_clientes_pagaron", p33_pct_clientes_pagaron, p66_pct_clientes_pagaron)
            )
        )
        .withColumn(
            "tend_pct_sin_deuda",
            _con_minimo(
                _tendencia_3("slope_pct_clientes_sin_deuda", p33_pct_clientes_sin_deuda, p66_pct_clientes_sin_deuda)
            )
        )
        .withColumn(
            "tend_pct_gestionados",
            _con_minimo(
                _tendencia_3("slope_pct_clientes_gestionados", p33_pct_clientes_gestionados, p66_pct_clientes_gestionados)
            )
        )
        .withColumn(
            "tend_lost_track",
            _con_minimo(
                _tendencia_3("slope_pct_clientes_lost_track", p33_pct_clientes_lost_track, p66_pct_clientes_lost_track)
            )
        )
        .withColumn(
            "tend_salidas",
            _con_minimo(
                _tendencia_3("slope_pct_salidas", p33_pct_salidas, p66_pct_salidas)
            )
        )
        .withColumn(
            "tend_entradas",
            _con_minimo(
                _tendencia_3("slope_pct_entradas", p33_pct_entradas, p66_pct_entradas)
            )
        )
        .withColumn(
            "tend_actived_plan",
            _con_minimo(
                _tendencia_3("slope_pct_actived_plan", p33_pct_actived_plan, p66_pct_actived_plan)
            )
        )
        .withColumn(
            "tend_saldo_x_cliente",
            _con_minimo(
                _tendencia_3("slope_saldo_x_cliente", p33_saldo_x_cliente, p66_saldo_x_cliente)
            )
        )
        .withColumn(
            "tend_roll_forward",
            _con_minimo(
                _tendencia_3("slope_pct_roll_forward", p33_pct_roll_forward, p66_pct_roll_forward)
            )
        )
        .withColumn(
            "tend_roll_backward",
            _con_minimo(
                _tendencia_3("slope_pct_roll_backward", p33_pct_roll_backward, p66_pct_roll_backward)
            )
        )
        .withColumn(
            "estabilidad_eficiencia",
            (
                F.when(
                    F.col("std_eficiencia").isNull(),
                    "Sin datos"
                ).when(
                    F.col("std_eficiencia") <= p50_std_eficiencia,
                    "Consistente"
                ).otherwise("Volatil")
            )
        )



        .withColumn(
            "score_tendencia",
            F.when(F.col("slope_eficiencia") >= p66_eficiencia, 1).otherwise(0) +
            F.when(F.col("slope_avg_fisematrasomax") <= p33_avg_fisematrasomax, 1).otherwise(0) +
            F.when(F.col("slope_pct_roll_forward") <= p33_pct_roll_forward, 1).otherwise(0)
        )
        .withColumn(
            "perfil_tendencia",
            (
                F.when(
                    F.col("score_tendencia") == 3, "Top"
                ).when(
                    F.col("score_tendencia") == 2, "Positiva"
                ).when(
                    F.col("score_tendencia") == 1, "Mixta"
                ).otherwise("En riesgo")
            )
        )

        .select(
            "fidespid",
            "fitipodepto",
            "tend_eficiencia",
            "tend_atraso",
            "tend_gestions",
            "tend_gestions_sin_cobro",
            "tend_pct_pagaron",
            "tend_pct_sin_deuda",
            "tend_pct_gestionados",
            "tend_lost_track",
            "tend_salidas",
            "tend_entradas",
            "tend_actived_plan",
            "tend_saldo_x_cliente",
            "tend_roll_forward",
            "tend_roll_backward",
            "estabilidad_eficiencia",
            "score_tendencia",
            "perfil_tendencia",

        )
    )

def _slope_col(y_col: str) -> F.Column:
    t = F.col("t")
    y = F.col(y_col)
    n = F.count(F.when(y.isNotNull(), 1))
    sum_t  = F.sum(t)
    sum_y  = F.sum(y)
    sum_ty = F.sum(t * y)
    sum_t2 = F.sum(t * t)
    num = n * sum_ty - sum_t * sum_y
    den = n * sum_t2 - sum_t * sum_t

    return F.when(den != 0, num / den).otherwise(F.lit(None))

def _tendencia_3(
    col_name : str,
    p33 : float,
    p66 : float,
) -> F.Column:
    return (
        F.when(
            F.col(col_name).isNull(),
            "Sin datos"
        ).when(
            F.col(col_name) < p33,
            "Decreciente"
        ).when(
            F.col(col_name) < p66,
            "Estable"
        ).otherwise("Creciente")
    )

def _con_minimo(col_tendencia: F.Column) -> F.Column:
    return (
        F.when(
            F.col("n_semanas_obs") < 2,
            "Insuficiente"
        ).otherwise(col_tendencia)
    )