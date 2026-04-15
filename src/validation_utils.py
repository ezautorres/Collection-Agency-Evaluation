from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
)
from typing import Tuple, Dict
from utils.generals import save_csv

def check_abt(df: DataFrame, csv_name: str):
    total_rows = df.count()
    total_despachos = df.select("fidespid").distinct().count()
    print("\nCombinaciones despacho-segmento :", total_rows)
    print("Despachos únicos                :", total_despachos)

    print("\nAlgunos valores:")
    df.groupBy("fitipodepto").count().orderBy(F.desc("count")).show()
    df.groupBy("estatus").count().orderBy(F.desc("count")).show()
    df.groupBy("fntipodespacho").count().orderBy(F.desc("count")).show()
    df.groupBy("fitipogestion").count().orderBy(F.desc("count")).show()
    df.groupBy("fihabilitadointerf").count().orderBy(F.desc("count")).show()

    summary = []
    for c in df.columns:
        summary.append(
            df.select(
                F.lit(c).alias("columna"),
                F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias("nulls"),
                F.countDistinct(F.col(c)).alias("distinct"),
                F.count(F.col(c)).alias("no_nulls")
            )
        )

    summary_df = summary[0]
    for s in summary[1:]:
        summary_df = summary_df.unionByName(s)
    summary_df = summary_df.withColumn(
        "pct_nulls",
        F.round(F.col("nulls") / F.lit(total_rows) * 100, 2)
    )
    summary_df.orderBy(F.desc("nulls")).show(len(df.columns), False)

    #save_csv(summary_df, csv_name)