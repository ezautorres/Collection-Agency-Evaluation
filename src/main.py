"""
main.py
-------

Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
import time
from pyspark.sql import (
    SparkSession,
    functions as F,
)
from utils.spark_utils import SparkManager, save_table
from utils.config import load_params, LoggerFactory

# Import parameters.
params = load_params()
week = params['semana']
n_weeks_backward = params['n_weeks_backward']
tbl_out = params['tbl_out']

def main(spark: SparkSession, log_path: str) -> None:

    # Create logger.
    logger = LoggerFactory.create_logger(
        name="baz",
        log_path=log_path,
    )

    # Period of interest.
    logger.info(
        "\n=== Parameters ===\n"
        "Current week         : %s\n"
        "History weeks        : %s\n"
        "Table out            : %s\n",
        week,
        n_weeks_backward,
        tbl_out
    )
    
    try:
        
        # Dataset.
        logger.info("Generating dataset...")

        # Applying model.
        logger.info("Running model...")

        # Last processing.
        logger.info("Processing final information...")

        logger.info("Process completed successfully.")
        
    except Exception as e:

        logger.error(f"It ended with an error: {str(e)}")

if __name__ == '__main__':

    start_time = time.time()
    
    spark = SparkManager.getSparkSession("MED")
    main(spark, log_path="../logs.log")
    spark.stop()
    end_time = time.time()

    print(f"[INFO] Total execution time: {end_time - start_time:.2f} seconds")