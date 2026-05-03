"""
Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
import time
from pyspark.sql import DataFrame, functions as F
from functools import reduce
from pyspark.sql.types import NumericType

def clean_agency_base(df: DataFrame) -> DataFrame:
    """
    Clean the agency base DataFrame.
    """
    df = _drop_mostly_null_rows(df)
    df = _fix_empresascobext_nulls(df)
    df = _fix_status_date_inconsistencies(df)
    df = _clean_capacity_manager_columns(df)
    
    return df


def _drop_mostly_null_rows(df: DataFrame) -> DataFrame:
    """
    Drop rows that have mostly null values.
    """
    # Threshold.
    n_cols = len(df.columns)
    threshold = n_cols / 2 + 1

    # List of expressions to count null values.
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


def _fix_empresascobext_nulls(df: DataFrame) -> DataFrame:
    """
    Fix null values in the fcempresascobext column.
    """
    return (
        df
        .withColumn(
            "fcempresascobext",
            F.when(
                F.col("fcempresascobext").isNull(),
                F.lit("")
            ).otherwise(F.col("fcempresascobext"))
        )
    )


def _fix_status_date_inconsistencies(df: DataFrame) -> DataFrame:
    """
    Fix inconsistencies in the status and date columns.
    """
    return (
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


def _clean_capacity_manager_columns(df: DataFrame) -> DataFrame:
    """
    Clean the capacity manager columns by replacing 0 values with null and
    removing outliers.
    """
    cols = ["ideal_cap_manager", "min_cap_manager", "max_cap_manager"]

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