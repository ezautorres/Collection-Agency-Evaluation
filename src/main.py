"""
main.py
-------

Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
import time
from pyspark.sql import SparkSession
from utils.spark_utils import (
    SparkManager,
    register_udf,
    save_table,
)
from utils.generals import save_csv
from utils.config import LoggerFactory, load_params
from data.dataset import get_dataset
from data.dataset_utils import get_week_around
from models.model import score_despachos

# Import parameters.
params = load_params()
week = params['semana']
n_weeks_backward = params['n_weeks_backward']
segm_legal = params['segm_legal']
tbl_out = params['tbl_out']

def main(
    spark: SparkSession,
    log_path: str = "../logs.log",
) -> None:
    start_time = time.time()
    print("")

    # Register jar for tacob decryption.
    register_udf(spark)

    # Create logger.
    logger = LoggerFactory.create_logger("MED", log_path)

    # Weeks boundaries.
    start_week = get_week_around(spark, week, n_weeks_backward - 1)
    start_week_ = get_week_around(spark, start_week, 1)

    # Period of interest.
    logger.info(
        "\n=== Parameters ===\n"
        "   Pivot week    : %s\n"
        "   History weeks : %s\n"
        "   Start week    : %s\n"
        "   Legal Segms.  : %s\n"
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
        data_hist, df = get_dataset(
            spark=spark,
            week=week,
            start_week=start_week,
            start_week_=start_week_,
            segm_legal=segm_legal,
        )
        save_table(data_hist, tbl_out + "_historia")
        save_table(df, tbl_out)
        logger.info("Dataset loaded successfully.")

        # Applying model.
        logger.info("Running model.")
        #scored = score_despachos(
        #    df,
        #    particion_fitipodepto=False,
        #    ajuste_dificultad=False,
        #)
        #cols_resumen = [
        #    "fidespid", "fitipodepto",
        #    
        #    # Score
        #    "score_final",
        #    "rank_despacho",
        #    "segmento_despacho",
        #    "perfil_tendencia",
#
        #    # Contexto
        #    "n_clientes_total",
#
        #    # Resultado financiero
        #    "eficiencia_cobranza",
        #    "pct_clientes_pagaron",
        #    "pct_semanas_con_pago",
#
#
        #    "avg_roll_forward",      
        #    "avg_roll_backward",      
        #    "avg_delta_atraso_neto",      
        #    "avg_atraso_salida",      
#
        #    # Gestión
        #    "pct_filas_con_gestion",
#
        #    # Tendencias
        #    "slope_eficiencia",
        #    
        #    
        #    # Categóricas de apoyo
        #    "cat_eficiencia_cobranza",
        #]
        #out = scored.select(*cols_resumen).orderBy("rank_despacho")
        #save_csv(out, "saida_ejemplo.csv")

        # Last processing.
        logger.info("Processing final information.")
        #save_table(df, tbl_out)

        # Log completion.
        logger.info("Process completed successfully.")
        
    except Exception as e:

        logger.error(f"An error occurred: {str(e)}")
    
    end_time = time.time()
    logger.info(f"[INFO] Total time: {end_time - start_time:.2f} s.")

if __name__ == '__main__':
    
    spark = SparkManager.getSparkSession("MED")
    main(spark)
    spark.stop()