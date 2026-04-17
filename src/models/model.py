"""
model.py
--------
Main code for the model.

Author: Ezau Faridh Torres Torres.
Date: April 2026.
"""
# Necessary imports.
from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

features = [
    "fncapacidadideal",
    "fncapacidadmin",
    "fncapacidadmax",
    "fnclientesactual",
    "fntipodespacho",
    "fitipogestion",
    "fitiempomanejocobza",
    "fihabilitadointerf",
    "has_empresas_cobext",
    "has_phone",
    "has_cp",
    "has_razon_social",
    "antiguedad_alta_dias",
    "clientes_x_gestor",
    "capacidad_min_sobre_ideal",
    "capacidad_min_sobre_max",
    "rango_capacidad",
    "rango_capacidad_sobre_ideal",
    "rango_capacidad_sobre_max",
    "telefono_valido_10_12_flag",
    "cp_5digitos_flag",
    "clientes_x_antiguedad",
    "info_basica_score"
]


def primer_modelo(df_model: DataFrame):
    print("Entran a modelo: ", df_model.count())
    df_model = df_model.dropna()
    print("Entran a modelo: ", df_model.count())
    df_model = df_model.filter(
        (F.col("fnclientesactual") >= 10) & 
        (F.col("fnclientesactual") <= 500000)
    )
    #df_model = df_model.select(
    #    *[F.col(c).cast("double").alias(c) for c in features],
    #    "fidespid", "fitipodepto"
    #)
    #df_model = df_model.fillna(df_model.select([F.mean(c).alias(c) for c in features]).first().asDict())

    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features_vec"
    )
    df_vec = assembler.transform(df_model)
    scaler = StandardScaler(
        inputCol="features_vec",
        outputCol="features_scaled",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(df_vec)
    df_scaled = scaler_model.transform(df_vec)

    kmeans = KMeans(
        k=3,
        seed=42,
        featuresCol="features_scaled",
        predictionCol="cluster"
    )

    model = kmeans.fit(df_scaled)
    df_clustered = model.transform(df_scaled)

    return df_clustered

def check_results(df_clustered: DataFrame):
    df_clustered.groupBy("cluster").count().show()
    df = (
        df_clustered
        .groupBy("cluster")
        .agg(
            F.count("*").alias("n_registros"),
            F.countDistinct("fidespid").alias("n_despachos"),
            F.max(F.col("fitipodepto")).alias("tipo_depto_max"),
            F.avg("fnclientesactual").cast("int").alias("clientes_actuales"),
            F.avg("fncapacidadmax").cast("int").alias("capacidad_maxima"),
            F.avg("fncapacidadideal").cast("int").alias("capacidad_ideal"),
            F.avg("fncapacidadmin").cast("int").alias("capacidad_admin"),
            F.avg("fitiempomanejocobza").alias("tiempo_manejo_cobza"),
            F.avg("antiguedad_alta_dias").cast("int").alias("antiguedad_dias"),
            F.avg("clientes_x_gestor").alias("clientes_x_gestor"),
            F.max("fntipodespacho").alias("tipo_despacho_max"),
            F.max("fitipogestion").alias("tipo_gestion_max"),
            F.avg("info_basica_score").alias("info_basica_score")
        ).orderBy("cluster")
    )
    return df