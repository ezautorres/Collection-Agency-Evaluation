"""
dataset.py
----------
Author: Ezau Faridh Torres Torres.
Date: April 2026.

Functions
---------
- get_dataset :
    Retrieve the dataset for the specified parameters.
"""
# Necessary imports.
from typing import List
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
)
from .agency.collection_agency import create_agency_base
from .agency.cleaning import clean_agency_base
from .agency.preprocessing import preprocess_agency_base
from .agency.features import create_agency_features
from .history.collection import create_historical_base
from .history.features import create_features

def get_dataset(
    spark: SparkSession,
    week: int,
    start_week: int,
    start_week_: int,
    segm_legal: List[int],
) -> DataFrame:
    agency_base = _get_agency_base(spark=spark, segm_legal=segm_legal)
    data_hist, historical_data = _get_historical_data(
        spark=spark,
        week=week,
        start_week=start_week,
        start_week_=start_week_,
        segm_legal=segm_legal,
    )

    df = agency_base.join(historical_data, on=["fidespid", "fitipodepto"], how="inner")

    return data_hist, df

def _get_agency_base(spark: SparkSession, segm_legal: List[int]) -> DataFrame:
    """
    Retrieve the agency base dataset for the specified parameters.
    """
    agency_base = create_agency_base(spark, segm_legal)
    agency_base = clean_agency_base(agency_base)
    agency_base = preprocess_agency_base(agency_base)
    agency_base = create_agency_features(agency_base)

    return agency_base

def _get_historical_data(
    spark: SparkSession,
    week: int,
    start_week: int,
    start_week_: int,
    segm_legal: List[int],
) -> DataFrame:
    """
    Retrieve the historical data for the specified parameters.
    """
    data_hist = (
        create_historical_base(spark, start_week_, segm_legal)
        .where(F.col("fisemana").between(start_week, week))
    )
    df = create_features(data_hist, segm_legal)

    return data_hist, df