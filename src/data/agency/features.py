"""
Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
from typing import Tuple
from pyspark.sql import DataFrame, functions as F

def create_agency_features(df: DataFrame) -> DataFrame:
    """
    Create agency features.
    """
    df = _add_valid_numerical_features(df, "fctelefono1", "phone", (10, 12))
    df = _add_valid_numerical_features(df, "fccodpostal", "cp", (5, 5))
    df = _add_valid_string_features(df, "fcempresascobext", "ext_collection")
    df = _add_valid_string_features(df, "fcnombrerazonsocial", "razon_social")
    df = _add_alphanumeric_features(df, "fcemail")
    df = _add_alphanumeric_features(df, "fcrfc")
    df = _add_alphanumeric_features(df, "fchomoclave")
    df = _add_date_features(df)
    df = _add_manager_capacity_features(df)
    
    return df


def _add_valid_numerical_features(
    df: DataFrame,
    col: str,
    name: str,
    length_range: Tuple,
) -> DataFrame:
    """
    Create valid numerical features.
    """
    min_len, max_len = length_range
    return (
        df
        .withColumn(
            f"valid_{name}",
            F.when(
                F.col(col).rlike(f"^[0-9]{{{min_len},{max_len}}}$"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop(col)
    )


def _add_valid_string_features(
    df: DataFrame,
    col: str,
    name: str,
) -> DataFrame:
    """
    Create valid string features.
    """ 
    trimmed_col = F.trim(F.col(col))
    return (
        df
        .withColumn(
            f"has_{name}",
            F.when(
                (F.length(trimmed_col) > 5) & 
                (trimmed_col.rlike(r"[A-Za-z]")), 
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop(col)
    )


def _add_alphanumeric_features(df: DataFrame, col: str) -> DataFrame:
    """
    Create alphanumeric features.
    """
    if col == "fcemail":
        pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$"
        name = "email"
    if col == "fcrfc":
        pattern = r"(?i)^[A-ZÑ&]{3,4}\d{6}$"
        name = "rfc"
    if col == "fchomoclave":
        pattern = r"^[A-Za-z0-9]{3}$"
        name = "homoclave"
    
    return (
        df
        .withColumn(
            f"valid_{name}",
            F.when(
                F.trim(
                    F.regexp_replace(F.col(col), r"[\t\n\r]", "")
                ).rlike(pattern), 
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop(col)
    )


def _add_date_features(df: DataFrame) -> DataFrame:
    """
    Create date features.
    """
    return (
        df
        .withColumn(
            "seniority_days",
            F.when(
                F.col("fdfechabaja").isNotNull() &
                F.col("fdfechaalta").isNotNull(),
                F.datediff(
                    F.to_date(F.from_unixtime(F.col("fdfechabaja") / 1000)),
                    F.to_date(F.from_unixtime(F.col("fdfechaalta") / 1000))
                )
            ).otherwise(F.lit(0))
        )
        .drop("fdfechabaja", "fdfechaalta")
    )


def _add_manager_capacity_features(df: DataFrame) -> DataFrame:
    """
    Create manager capacity features.
    """
    return (
        df
        .withColumn(
            "min_over_ideal_cap_manager",
            F.when(
                F.col("ideal_cap_manager") > 0,
                F.col("min_cap_manager") / F.col("ideal_cap_manager")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "min_over_max_cap_manager",
            F.when(
                F.col("max_cap_manager") > 0,
                F.col("min_cap_manager") / F.col("max_cap_manager")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "rang_cap_manager",
            F.col("max_cap_manager") - F.col("min_cap_manager")
        )
        .withColumn(
            "rang_over_ideal_cap_manager",
            F.when(
                F.col("ideal_cap_manager") > 0,
                F.col("rang_cap_manager") / F.col("ideal_cap_manager")
            )
        )
        .withColumn(
            "rang_over_max_cap_manager",
            F.when(
                F.col("max_cap_manager") > 0,
                F.col("rang_cap_manager") / F.col("max_cap_manager")
            )
        )
    )