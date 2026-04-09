"""
spark_utils.py
--------------
Module with utility functions for Spark session management and DataFrame
operations.

Author: Ezau Faridh Torres Torres.
Date: March 2026.

Functions
---------
- getSparkSession :
    Creates or retrieves a Spark Session.
- register_voltage :
    Registers a temporary UDF in the Spark session.
- save_table :
    Saves a DataFrame as a Hive table.
"""
# Necessary imports.
import os
from pyspark.sql import (
    SparkSession,
    DataFrame,
)

class SparkManager:
    
    @staticmethod
    def getSparkSession(app_name: str) -> SparkSession:
        """
        Creates a Spark Session with name 'app_name' and avoids recreate it
        is called.

        Parameters
        ----------
        app_name : str
            Name to the session.

        Return
        ------
        spark : SparkSession
            Spark Session created.
        """
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        SparkManager._spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.ui.enabled", "false")
            .config("spark.executorEnv.PYTHONPATH", base_dir)
            .config("spark.driverEnv.PYTHONPATH", base_dir)
            .getOrCreate()
        )

        return SparkManager._spark
    
        
def save_table(table: DataFrame, name: str) -> None:
    """
    Save a DataFrame as a Hive table with configurable mode and partitions.

    Parameters
    ----------
    table : DataFrame
        Spark DataFrame to save.
    name : str
        Name of the Hive table.
    """
    writer = table.write.mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    )
    writer.saveAsTable(name)
    print(f"Table saved as:\n   {name}")