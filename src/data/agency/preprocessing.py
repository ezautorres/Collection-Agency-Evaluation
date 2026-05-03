"""
Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
from pyspark.sql import DataFrame, functions as F


def preprocess_agency_base(df: DataFrame) -> DataFrame:
    """
    Preprocess the agency base DataFrame.
    """
    df = _impute_gestor_capacity_fields(df)
    df = _enforce_capacity_bounds(df)
    
    return df


def _impute_gestor_capacity_fields(df: DataFrame) -> DataFrame:
    """
    Impute missing values in capacity fields with medians.
    """
    cols = ["ideal_cap_manager", "min_cap_manager", "max_cap_manager"]

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


def _enforce_capacity_bounds(df: DataFrame) -> DataFrame:
    """
    Enforce bounds on the capacity manager columns.
    """
    return (
        df
        .withColumn(
            "min_cap_manager",
            F.least(F.col("min_cap_manager"), F.col("ideal_cap_manager"))
        )
        .withColumn(
            "max_cap_manager",
            F.greatest(F.col("max_cap_manager"), F.col("ideal_cap_manager"))
        )
    )