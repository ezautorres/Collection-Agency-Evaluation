"""
clients.py
----------
Creates the main ABT of clients allowed.

Author: Ezau Faridh Torres Torres.
Date: April 2026.

Functions
---------
- get_dataset :
    Retrieve the dataset for the specified parameters.
"""
# Necessary imports.
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
)

def get_dataset(
    spark: SparkSession,
) -> DataFrame:
    pass