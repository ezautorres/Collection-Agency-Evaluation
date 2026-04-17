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
from utils.spark_utils import (
    SparkManager,
    register_udf,
    save_table,
)
from utils.config import (
    LoggerFactory,
    load_params,
    load_contact_codes
)
from data.dataset import get_dataset
from utils.generals import save_csv
from data.dataset_utils import get_week_around, get_week_limits
from validation_utils import check_abt
from models.model import primer_modelo, check_results

# Import parameters.
params = load_params()
contact_codes = load_contact_codes()
week = params['semana']
n_weeks_backward = params['n_weeks_backward']
segm_legal = params['segm_legal']
tbl_out = params['tbl_out']

def main(
    spark: SparkSession,
    log_path: str = "../logs.log",
) -> None:
    print("")

    # Register jar for tacob.
    register_udf(spark)

    # Create logger.
    logger = LoggerFactory.create_logger(
        name="MED",
        log_path=log_path,
    )

    start_week = get_week_around(spark, week, n_weeks_backward)
    # Period of interest.
    logger.info(
        "\n=== Parameters ===\n"
        "   Current week  : %s\n"
        "   History weeks : %s\n"
        "   Start week    : %s\n"
        "   Segms. legal  : %s\n"
        "   Table out     : %s\n",
        week,
        n_weeks_backward,
        start_week,
        segm_legal,
        tbl_out
    )
    
    try:
        
        # Dataset.
        logger.info("Generating dataset.")
        df = get_dataset(
            spark,
            week=week,
            start_week=start_week,
            segm_legal=segm_legal,
        )
        #df = df.where(F.col("estatus")==1)
        #print(df.count())
        save_table(df, tbl_out + "_dataset")
        logger.info("Dataset loaded successfully.")
        #check_abt(df, "check_data.csv")

        # Applying model.
        logger.info("Running model.")
        #df = primer_modelo(df)
        #print(df.count())
        #df2 = check_results(df)
        #save_csv(df2, "clusters.csv")


        # Last processing.
        logger.info("Processing final information.")
        #save_table(df, tbl_out)

        # Log completion.
        logger.info("Process completed successfully.")
        
    except Exception as e:

        logger.error(f"An error occurred: {str(e)}")

if __name__ == '__main__':

    start_time = time.time()
    
    spark = SparkManager.getSparkSession("MED")
    main(spark)
    spark.stop()
    end_time = time.time()

    print(f"\n[INFO] Total execution time: {end_time - start_time:.2f} seconds.")