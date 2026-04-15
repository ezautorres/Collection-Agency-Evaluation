# Necessary imports.
import os
from pyspark.sql import DataFrame

def save_csv(df: DataFrame, csv_name: str) -> None:
    """
    """
    abs_path = os.path.abspath(f"{csv_name}")
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    df.toPandas().to_csv(abs_path, index=False)