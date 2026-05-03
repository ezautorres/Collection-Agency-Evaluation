# Necessary imports.
from pyspark.sql import DataFrame, functions as F
from typing import Dict


def assign_contact_type(df: DataFrame, contact_codes: Dict) -> DataFrame:
    """
    Assign contact type based on visit and gestion codes.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with 'fivisitaid' and 'figestionid' columns.
    contact_codes : Dict
        Dictionary mapping contact types to visit and gestion codes.

    Returns
    -------
    DataFrame
        DataFrame with an additional 'contact_type' column.
    """
    df = df.withColumn("contact_type", F.lit("sin_info"))

    for contact_type, codes in contact_codes.items():
        for fivisitaid, _codes in codes.items():
            df = df.withColumn(
                "contact_type",
                F.when(
                    (F.col("fivisitaid") == fivisitaid) &
                    (F.col("figestionid").isin(_codes)),
                    F.lit(contact_type)
                ).otherwise(F.col("contact_type"))
            )

    return df