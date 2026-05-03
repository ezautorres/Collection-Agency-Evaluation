from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import Dict


DEFAULT_METRICAS = {
    "info_fiscal_completa":     {"peso": 5, "direccion":  1, "activa": True},
    "eficiencia":               {"peso": 5, "direccion":  1, "activa": True},
    "pct_clientes_pagaron":     {"peso": 4, "direccion":  1, "activa": True},
    "pct_clientes_gestionados": {"peso": 3, "direccion":  1, "activa": True},
    "avg_fisematrasomax":       {"peso": 3, "direccion": -1, "activa": True},
    "pct_roll_forward":         {"peso": 2, "direccion": -1, "activa": True},
    "pct_clientes_lost_track":  {"peso": 2, "direccion": -1, "activa": True},
    "pct_clientes_sin_deuda":   {"peso": 2, "direccion":  1, "activa": True},
    "gestiones_x_cliente":      {"peso": 1, "direccion":  1, "activa": True},
    "pct_gestiones_efectivas":  {"peso": 1, "direccion":  1, "activa": False},
}

def score_despachos(df: DataFrame, metricas: Dict = None) -> DataFrame:

    metricas = metricas if metricas else DEFAULT_METRICAS
    
    activas = {
        col: cfg
        for col, cfg in metricas.items()
        if cfg.get("activa", True) and col in df.columns
    }

    peso_total = sum(cfg["peso"] for cfg in activas.values())
    pesos_norm = {col: cfg["peso"] / peso_total for col, cfg in activas.items()}
    df_conf = df
    w_pct = Window.partitionBy(["fitipodepto"])

    result = df_conf
    score_expr = F.lit(0.0)

    for col, cfg in activas.items():
        pct_col = f"score_{col}_pct"
        direccion = cfg["direccion"]

        order_expr = (
            F.col(col).asc_nulls_last()
            if direccion == -1
            else F.col(col).desc_nulls_last()
        )
        pct_window = w_pct.orderBy(order_expr)

        result = result.withColumn(
            pct_col,
            F.when(
                F.col(col).isNotNull(),
                F.percent_rank().over(pct_window) * 100
            ).otherwise(F.lit(None))
        )
        score_expr = score_expr + F.when(
            F.col(pct_col).isNotNull(),
            F.col(pct_col) * pesos_norm[col]
        ).otherwise(F.lit(0.0))

    result = result.withColumn("score_final", score_expr)

    w_rank = (
        Window
        .partitionBy(["fitipodepto"])
        .orderBy(F.col("score_final").desc_nulls_last())
    )

    result = result.withColumn("rank", F.rank().over(w_rank))
    result = result.withColumn(
        "segmento_despacho",
        F.when(F.col("score_final") >= 80, "Élite")
         .when(F.col("score_final") >= 60, "Buen desempeño")
         .when(F.col("score_final") >= 40, "Desempeño medio")
         .when(F.col("score_final") >= 20, "Bajo desempeño")
         .otherwise("Crítico")
    )

    return result