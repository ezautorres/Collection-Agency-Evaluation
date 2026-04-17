"""
Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
import os
import time
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    Window,
)
from typing import List
from functools import reduce
from pyspark.sql.types import NumericType

def drop_mostly_null_rows(df: DataFrame) -> DataFrame:
    """
    Drop rows that have mostly null values.
    """
    # Calculate the threshold for dropping rows.
    n_cols = len(df.columns)
    threshold = n_cols / 2 + 1

    # Create a list of expressions to count null values.
    exprs = []
    for field in df.schema.fields:
        c = F.col(field.name)

        # Count null values.
        if isinstance(field.dataType, NumericType):
            exprs.append(F.when(c.isNull() | (c == 0), 1).otherwise(0))
        else:
            exprs.append(F.when(c.isNull(), 1).otherwise(0))

    # Sum the null value counts.
    empty_count = reduce(lambda a, b: a + b, exprs)
    
    return df.where(empty_count <= threshold)

def fix_empresascobext_nulls(df: DataFrame) -> DataFrame:
    """
    Fix null values in the fcempresascobext column.
    """
    df = (
        df.withColumn(
            "fcempresascobext",
            F.when(
                F.col("fcempresascobext").isNull(),
                F.lit("")
            ).otherwise(F.col("fcempresascobext"))
        )
    )
    return df

def fix_status_date_inconsistencies(df: DataFrame) -> DataFrame:
    """
    Fix inconsistencies in the status and date columns.
    """
    df = (
        df
        .withColumn(
            "fdfechabaja",
            F.when(
                F.col("estatus") == 1,
                F.lit(None)
            ).when(
                (F.col("estatus") == 0) & F.col("fdfechabaja").isNull(),
                F.lit(int(time.time() * 1000))
            ).otherwise(F.col("fdfechabaja"))
        )
    )
    
    return df

def clean_capacity_columns(df: DataFrame) -> DataFrame:
    """
    Clean the capacity columns by replacing 0 values with null and
    removing outliers.
    """
    cols = ["fncapacidadideal", "fncapacidadmin", "fncapacidadmax"]

    # Calculate percentiles.
    quantiles = df.approxQuantile(cols, [0.95], 0.01)
    p = {col: q[0] for col, q in zip(cols, quantiles)}

    for col in cols:
        df = (
            df
            .withColumn(
                col,
                F.when(
                    F.col(col) == 0,
                    F.lit(None)
                ).when(
                    F.col(col) > F.lit(p[col]),
                    F.lit(None)
                ).otherwise(F.col(col))
            )
        )

    return df


def enforce_capacity_bounds(df: DataFrame) -> DataFrame:
    df = (
        df
        .withColumn(
            "fncapacidadmin",
            F.least(F.col("fncapacidadmin"), F.col("fncapacidadideal"))
        )
        .withColumn(
            "fncapacidadmax",
            F.greatest(F.col("fncapacidadmax"), F.col("fncapacidadideal"))
        )
    )
    return df