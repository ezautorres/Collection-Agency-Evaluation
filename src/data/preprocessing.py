"""
Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
from pyspark.sql import (
    DataFrame,
    functions as F,
)

def impute_gestor_capacity_fields(df: DataFrame) -> DataFrame:
    """
    Impute missing values in capacity fields with medians.
    """
    cols = ["fncapacidadideal", "fncapacidadmin", "fncapacidadmax"]

    # Median calculation.
    exprs = [
        F.expr(f"percentile_approx({c}, 0.5)").alias(f"{c}_median")
        for c in cols
    ]
    medians = df.groupBy("fitipodepto").agg(*exprs)

    df = df.join(medians, on="fitipodepto", how="left")

    # Impute missing values with medians.
    for c in cols:
        df = (
            df
            .withColumn(
                c,
                F.coalesce(F.col(c), F.col(f"{c}_median"))
            )
        )

    return df.drop(*[f"{c}_median" for c in cols])