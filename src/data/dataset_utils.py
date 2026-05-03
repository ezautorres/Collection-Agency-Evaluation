"""
dataset_utils.py
----------------
Contains utility functions for creating and manipulating datasets.

Author: Ezau Faridh Torres Torres.
Date: April 2026.

Functions
---------
- get_week_around :
    Returns the week around a pivot week.
- get_week_limits :
    Returns the start and end dates of a given week.
"""
# Necessary imports.
from pyspark.sql import SparkSession, functions as F
from typing import Tuple

def get_week_around(
    spark: SparkSession,
    pivot_week: int,
    n: int,
    forward: bool = False,
) -> int:
    """
    Returns the week around a pivot week.

    Parameters
    ----------
    spark : SparkSession
        The Spark session.
    pivot_week : int
        The pivot week.
    n : int
        The number of weeks to retrieve.
    forward : bool, optional
        Whether to retrieve the week forward or backward from the pivot
        week. (default = False).

    Returns
    -------
    int
        The week around the pivot week.
    """
    # Build num_periodo_sem_ = YYYYWW as integer.
    df = (
        spark.table("rd_baz_bdclientes.rd_cat_tiempo")
        .select(
            F.concat(
                F.col("nanioekt"),
                F.lpad(F.col("nsemanaekt"), 2, "0")
            ).cast("int").alias("num_periodo_sem_")
        )
        .distinct()
    )

    # Choose filter and ordering direction.
    if forward:
        filter_expr = F.col("num_periodo_sem_") >= pivot_week + 1
        order_expr = F.col("num_periodo_sem_").asc()
    else:
        filter_expr = F.col("num_periodo_sem_") <= pivot_week - 1
        order_expr = F.col("num_periodo_sem_").desc()

    # Filter, order, limit, and collect the weeks into a list.
    df_row = (
        df
        .filter(filter_expr)
        .orderBy(order_expr)
        .limit(n)
        .agg(F.collect_list("num_periodo_sem_").alias("weeks"))
        .first()
    )

    return df_row.weeks[-1] if df_row and df_row.weeks else []


def get_week_limits(
    spark: SparkSession,
    fisemana: int
) -> Tuple[int, int]:
    """
    Returns the start and end dates of a given week in format YYYYMMDD.

    Parameters
    ----------
    spark : SparkSession
        The Spark session.
    fisemana : int
        The week for which to retrieve dates.

    Returns
    -------
    Tuple[int, int]
        The start and end dates of the given week in format YYYYMMDD.
    """
    row = (
        spark.table("rd_baz_bdclientes.rd_fechas_semanas")
        .withColumn(
            "yearweek",
            F.concat(
                F.col("fianioproceso"),
                F.lpad(F.col("fisemproceso"), 2, "0")
            ).cast("int")
        )
        .where(F.col("yearweek") == fisemana)
        .select("fdfecinicial", "fdfectermino")
        .first()
    )

    if row is None:
        return None, None

    return row["fdfecinicial"], row["fdfectermino"]