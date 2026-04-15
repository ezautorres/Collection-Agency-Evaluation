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
import os
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    Window,
)
from typing import List
from .dataset_utils import assign_contact_type
from .collection_agency import create_agency_base
from .historical_features import build_agency_history

def get_dataset(
    spark: SparkSession,
    week: int,
    start_week: int,
    segm_legal: List[int],  
) -> DataFrame:
    
    despachos = create_agency_base(spark, segm_legal)
    historial = build_agency_history(spark, week, start_week, segm_legal)
    df_final = despachos
    
    return df_final

def metricas(
    spark: SparkSession,
    fase: str,
    fisemana: int,
    fideptos: List[int],
    contact_codes,
) -> DataFrame:
    init_week = fisemana - 70
    
    df_entrada = (
        spark.table("rd_baz_bdclientes.rd_clasificacteu")
        .where(
            (F.col("fisemana") == F.lit(fisemana)) &
            (F.col("fitipodepto") == 1) &
            (F.col("fideptoid").isin([int(x) for x in fideptos])) &
            (F.col("fisematrasomax").between(2,6))
        )
        .select(
            
            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).cast("string").alias("id_cliente"),
            
            "fideptoid",
        )
        .dropDuplicates(["id_cliente"])
    )
    
    # TACOB
    tacob_raw = (
        spark.table("rd_baz_bdclientes.rd_tacobgpshh")
        .where(
            (F.col("fisemana") >= F.lit(init_week)) &
            (F.col("fivisitaid") < 200) &
            (F.col("fivisitaid") != 50)
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                "fipaiscu",
                "ficanalcu",
                "fisucursalcu",
                "fifoliocu"
            ).alias("id_cliente"),

            # Latitude and longitude of client.
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclatitudcliente as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclatitudcliente', 1, '0.0'"
                ")"
            ).alias("lat_cliente"),
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclongitudcliente as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclongitudcliente', 1, '0.0'"
                ")"
            ).alias("lon_cliente"),

            # Latitude and longitude of GPS.
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclatitudgps as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclatitudgps', 1, '0.0'"
                ")"
            ).alias("lat_gestor"),
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclongitudgps as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclongitudgps', 1, '0.0'"
                ")"
            ).alias("lon_gestor"),
            
            # Duration of the gestion in minutes.
            (
                (F.col("fdfecfingest") - F.col("fdfeciniciogest")) / 60000
            ).alias("minutos"),

            # Other relevant columns.
            "fisemana",
            "fivisitaid",
            "figestionid",
        )

        # Filtro solo en df_entrada.
        .join(df_entrada, on="id_cliente", how="inner")
        
        # Delete rows with lat/lon = 0.0.
        .where(
            (F.col("lat_gestor") != "0.0") &
            (F.col("lon_gestor") != "0.0") &
            (F.col("lat_cliente") != "0.0") &
            (F.col("lon_cliente") != "0.0")
        )

        # Distance calculation using Haversine formula.
        .withColumn(
            "dist_m",
            F.round(
                6_371_000
                * F.acos(
                    F.cos(F.radians(F.col("lat_gestor").cast("double")))
                    * F.cos(F.radians(F.col("lat_cliente").cast("double")))
                    * F.cos(
                        F.radians(F.col("lon_cliente").cast("double"))
                        - F.radians(F.col("lon_gestor").cast("double"))
                    )
                    + F.sin(F.radians(F.col("lat_gestor").cast("double")))
                    * F.sin(F.radians(F.col("lat_cliente").cast("double")))
                ),
                2,
            ),
        )

        # Filter.
        .where(
            (F.col("dist_m") < 100) &
            (F.col("minutos") >= 1)
        )
    )

    tacob = (
        assign_contact_type(tacob_raw, contact_codes)
        .withColumn(
            "contacto_directo",
            F.when(
                F.col("contact_type") == "contacto_directo",
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        # Agrupado.
        .groupBy("id_cliente", "fisemana")
        .agg(
            F.count(F.lit(1)).alias("n_visitas"),
            F.sum("contacto_directo").alias("n_contactos_directos"),
        )
    )

    recup = (
        spark.table("rd_baz_bdclientes.rd_recuperacion_cuotas_cobranza")
        .where(
            (F.col("fisemana") >= F.lit(init_week)) &
            (F.col("fideptoid").isin([int(x) for x in fideptos]))
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                "fipaiscu",
                "ficanalcu",
                "fisucursalcu",
                "fifoliocu"
            ).alias("id_cliente"),

            "fisemana",
            F.greatest(
                F.coalesce(F.col("fdcreccapital"), F.lit(0)),
                F.lit(0)
            ).alias("capital"),
            F.greatest(
                F.coalesce(F.col("fdcrecmoratorio"), F.lit(0)),
                F.lit(0)
            ).alias("moratorio"),
        )

        .join(df_entrada, on="id_cliente", how="inner")

        # Agrupado.
        .groupBy("id_cliente", "fisemana")
        .agg(
            (F.sum("capital") + F.sum("moratorio")).alias("cobranza")
        )
    )

    cteu_hist = (
        spark.table("rd_baz_bdclientes.rd_clasificacteu")
        .where(
            (F.col("fisemana") >= F.lit(init_week)) &
            (F.col("fideptoid").isin([int(x) for x in fideptos]))
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).alias("id_cliente"),

            "fisemana",
            "fisematrasomax",
            "fnultimoabono",
            "fnultabonomora",
        )
        
        .join(df_entrada, on="id_cliente", how="inner")
        .dropDuplicates(["id_cliente", "fisemana"])
    )

    # Ventanas semanales.
    w = (
        Window
        .partitionBy("id_cliente")
        .orderBy(F.col("fisemana").cast("int"))
    )

    cteu_feat = (
        cteu_hist
        .withColumn(
            "abono",
            F.greatest(
                F.coalesce("fnultimoabono", F.lit(0)),
                F.lit(0)
            )
        )
        .withColumn(
            "abono_mora",
            F.greatest(
                F.coalesce("fnultabonomora", F.lit(0)),
                F.lit(0)
            )
        )
        .withColumn(
            "pago_requerido_raw",
            F.col("abono") + F.col("abono_mora")
        )
        # lag de pago requerido (t-1)
        .withColumn(
            "pago_requerido",
            F.lag("pago_requerido_raw", 1).over(w)
        )
        # lag de atraso para roll forward
        .withColumn(
            "prev_atraso",
            F.lag("fisematrasomax", 1).over(w)
        )
        .withColumn(
            "roll_forward",
            F.when(
                (F.col("prev_atraso").isNotNull()) &
                (F.col("fisematrasomax") == F.col("prev_atraso") + 1),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "con_prev",
            F.col("prev_atraso").isNotNull().cast("int")
        )
        .join(tacob, on=["id_cliente", "fisemana"], how="left")
        .join(recup, on=["id_cliente", "fisemana"], how="left")
        .withColumn("n_visitas", F.coalesce("n_visitas", F.lit(0)))
        .withColumn(
            "n_contactos_directos",
            F.coalesce("n_contactos_directos", F.lit(0))
        )
        .withColumn("cobranza", F.coalesce("cobranza", F.lit(0)))
        .withColumn(
            "pago_requerido",
            F.coalesce("pago_requerido", F.lit(0))
        )
    )

    df = (
        cteu_feat
        .groupBy("fisemana")
        .agg(
            F.countDistinct("id_cliente").alias("n_clientes"),
            F.countDistinct("fideptoid").alias("n_deptos"),

            F.sum("n_visitas").alias("n_visitas"),
            F.sum("n_contactos_directos").alias("n_contactos_directos"),

            F.sum(
                F.col("cobranza").cast("decimal(38,2)")
            ).alias("cobranza_total"),
            F.sum(
                F.col("pago_requerido").cast("decimal(38,0)")
            ).alias("pago_requerido_total"),

            F.avg("fisematrasomax").alias("avg_fisematrasomax"),

            F.sum("roll_forward").alias("n_roll_forward"),
            F.sum("con_prev").alias("n_con_prev"),

            F.countDistinct(
                F.when(
                    F.col("n_visitas") > 0,
                    F.col("id_cliente")
                )
            ).alias("n_clientes_con_visita"),
        )
        .withColumn(
            "avg_visitas_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_visitas") / F.col("n_clientes")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "avg_contactos_directos_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_contactos_directos") / F.col("n_clientes")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "avg_visitas_x_cliente_con_visita",
            F.when(
                F.col("n_clientes_con_visita") > 0,
                F.col("n_visitas") / F.col("n_clientes_con_visita")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "pct_contacto_directo",
            F.when(
                F.col("n_visitas") > 0,
                F.col("n_contactos_directos") / F.col("n_visitas")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "pct_roll_forward",
            F.when(
                F.col("n_con_prev") > 0,
                F.col("n_roll_forward") / F.col("n_con_prev")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "eficiencia",
            F.when(
                F.col("pago_requerido_total") > 0,
                F.col("cobranza_total") / F.col("pago_requerido_total")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "fase",
            F.when(
                F.col("fisemana") == fisemana,
                F.lit(fase + "_i")
            ).otherwise(F.lit(fase))
        )
        .where(
            (F.col("fisemana") > init_week + 1) &
            (F.col("fisemana") <= 202609)
        )
        .orderBy("fisemana")
    )

    cols_order = [
        "fase", "fisemana", "n_deptos",
        "n_clientes", "n_visitas", "avg_visitas_x_cliente",
        "avg_visitas_x_cliente_con_visita", "n_contactos_directos",
        "pct_contacto_directo", "avg_contactos_directos_x_cliente",
        "cobranza_total", "pago_requerido_total", "eficiencia",
        "n_roll_forward", "n_con_prev", "pct_roll_forward",
        "avg_fisematrasomax", 
    ]

    abs_path = os.path.abspath(f"{fase}.csv")
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    df.select(*cols_order).toPandas().to_csv(abs_path, index=False)

    return df.select(*cols_order)

def metricas_iloc(
    spark: SparkSession,
    df: DataFrame,
    csv_name: str,
    init_week: int = 202604,
    end_week: int = 202609,
    only_vigentes: bool = False,
    only_no_vigentes: bool = False,
    contact_codes: List[int] = None
) -> None: 

    init_week_tmp = init_week - 1
    end_week_tmp = end_week + 1
    
    # Tacob info (visitas).
    tacob_raw = (
        spark.table("rd_baz_bdclientes.rd_tacobgpshh")
        .where(
            (F.col("fisemana") >= F.lit(init_week_tmp)) &
            (F.col("fisemana") <= F.lit(end_week_tmp)) &
            (F.col("fivisitaid") < 200) &
            (F.col("fivisitaid") != 50)
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                "fipaiscu",
                "ficanalcu",
                "fisucursalcu",
                "fifoliocu"
            ).alias("id_cliente"),

            # Latitude and longitude of client.
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclatitudcliente as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclatitudcliente', 1, '0.0'"
                ")"
            ).alias("lat_cliente"),
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclongitudcliente as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclongitudcliente', 1, '0.0'"
                ")"
            ).alias("lon_cliente"),

            # Latitude and longitude of GPS.
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclatitudgps as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclatitudgps', 1, '0.0'"
                ")"
            ).alias("lat_gestor"),
            F.expr(
                "bdf_voltage_simpleapi_v2("
                "cast(fclongitudgps as string),"
                "'rd_baz_bdclientes.rd_tacobgpshh',"
                "'fclongitudgps', 1, '0.0'"
                ")"
            ).alias("lon_gestor"),
            
            # Duration of the gestion in minutes.
            (
                (F.col("fdfecfingest") - F.col("fdfeciniciogest")) / 60000
            ).alias("minutos"),

            # Other relevant columns.
            "fisemana",
            "fivisitaid",
            "figestionid",
        )

        # Filtro solo en df.
        .join(df, on="id_cliente", how="inner")

        .where(
            (F.col("lat_gestor") != "0.0") &
            (F.col("lon_gestor") != "0.0") &
            (F.col("lat_cliente") != "0.0") &
            (F.col("lon_cliente") != "0.0")
        )

        # Distance calculation using Haversine formula.
        .withColumn(
            "dist_m",
            F.round(
                6_371_000
                * F.acos(
                    F.cos(F.radians(F.col("lat_gestor").cast("double")))
                    * F.cos(F.radians(F.col("lat_cliente").cast("double")))
                    * F.cos(
                        F.radians(F.col("lon_cliente").cast("double"))
                        - F.radians(F.col("lon_gestor").cast("double"))
                    )
                    + F.sin(F.radians(F.col("lat_gestor").cast("double")))
                    * F.sin(F.radians(F.col("lat_cliente").cast("double")))
                ),
                2,
            ),
        )

        # Filter.
        .where(
            (F.col("dist_m") < 100) &
            (F.col("minutos") >= 0.5)
        )
    )

    # Contact types.
    tacob = (
        assign_contact_type(tacob_raw, contact_codes)

        # Contacto directo.
        .withColumn(
            "contacto_directo",
            F.when(
                F.col("contact_type") == "contacto_directo",
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        # Contacto efectivo.
        .withColumn(
            "contacto_efectivo",
            F.when(
                (F.col("fivisitaid") == 1) |
                ((F.col("fivisitaid") == 9)  & (F.col("figestionid") == 3)) |
                ((F.col("fivisitaid") == 11) & (F.col("figestionid") == 4)) |
                ((F.col("fivisitaid") == 32) & (F.col("figestionid") == 7)),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        # Agrupado.
        .groupBy("id_cliente", "fisemana")
        .agg(
            F.count(F.lit(1)).alias("n_visitas"),
            F.sum("contacto_directo").alias("n_contactos_directos"),
            F.sum("contacto_efectivo").alias("n_contactos_efectivos")
        )
    )

    # Cobranza.
    recup = (
        spark.table("rd_baz_bdclientes.rd_recuperacion_cuotas_cobranza")
        .where(
            (F.col("fisemana") >= F.lit(init_week_tmp)) &
            (F.col("fisemana") <= F.lit(end_week_tmp))
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                "fipaiscu",
                "ficanalcu",
                "fisucursalcu",
                "fifoliocu"
            ).alias("id_cliente"),

            # Info.
            "fisemana",
            F.greatest(
                F.coalesce(F.col("fdcreccapital"), F.lit(0)),
                F.lit(0)
            ).alias("capital"),
            F.greatest(
                F.coalesce(F.col("fdcrecmoratorio"), F.lit(0)),
                F.lit(0)
            ).alias("moratorio"),
        )

        # Filtro solo en df.
        .join(df, on="id_cliente", how="inner")

        # Agrupado.
        .groupBy("id_cliente", "fisemana")
        .agg(
            (F.sum("capital") + F.sum("moratorio")).alias("cobranza")
        )
    )

    # Ventanas semanales.
    w = (
        Window
        .partitionBy("id_cliente")
        .orderBy(F.col("fisemana").cast("int"))
    )

    # Pago requerido y pivote.
    cteu = spark.table("rd_baz_bdclientes.rd_clasificacteu")
    if only_vigentes:
        cteu = cteu.where((F.col("fitipodepto") == 3))
    if only_no_vigentes:
        cteu = cteu.where((F.col("fitipodepto") != 3))

    cteu = (
        cteu
        .where(
            (F.col("fisemana") >= F.lit(init_week_tmp)) &
            (F.col("fisemana") <= F.lit(end_week_tmp))
        )
        .select(

            # ID Client.
            F.concat_ws(
                "-",
                F.col("fipais").cast("string"),
                F.col("ficanal").cast("string"),
                F.col("fisucursal").cast("string"),
                F.col("fifolio").cast("string"),
            ).alias("id_cliente"),

            # Info.
            "fisemana",
            "fnultimoabono",
            "fnultabonomora",
            "fisematrasomax"
        )

        # Filtro solo en df.
        .join(df, on="id_cliente", how="inner")
        .dropDuplicates(["id_cliente", "fisemana"])

        # Metrics.
        .withColumn(
            "abono",
            F.greatest(
                F.coalesce("fnultimoabono", F.lit(0)),
                F.lit(0)
            )
        )
        .withColumn(
            "abono_mora",
            F.greatest(
                F.coalesce("fnultabonomora", F.lit(0)),
                F.lit(0)
            )
        )
        .withColumn(
            "pago_requerido_raw",
            F.col("abono") + F.col("abono_mora")
        )
        # lag de pago requerido (t-1)
        .withColumn(
            "pago_requerido",
            F.lag("pago_requerido_raw", 1).over(w)
        )

        # Cruce con tacob y recup.
        .join(tacob, on=["id_cliente", "fisemana"], how="left")
        .join(recup, on=["id_cliente", "fisemana"], how="left")

        # Relleno de nulos
        .withColumn("n_visitas", F.coalesce("n_visitas", F.lit(0)))
        .withColumn(
            "n_contactos_directos",
            F.coalesce("n_contactos_directos", F.lit(0))
        )
        .withColumn(
            "n_contactos_efectivos",
            F.coalesce("n_contactos_efectivos", F.lit(0))
        )
        .withColumn("cobranza", F.coalesce("cobranza", F.lit(0)))
        .withColumn(
            "pago_requerido",
            F.coalesce("pago_requerido", F.lit(0))
        )
    )
    
    # Conteo final.
    df = (
        cteu
        .groupBy("fisemana")
        .agg(
            F.countDistinct("id_cliente").alias("n_clientes"),

            F.sum("n_visitas").alias("n_visitas"),
            F.sum("n_contactos_directos").alias("n_contactos_directos"),
            F.sum("n_contactos_efectivos").alias("n_contactos_efectivos"),

            F.sum(
                F.col("cobranza").cast("decimal(38,2)")
            ).alias("cobranza_total"),
            F.sum(
                F.col("pago_requerido").cast("decimal(38,0)")
            ).alias("pago_requerido_total"),
        )
        .withColumn(
            "avg_visitas_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_visitas") / F.col("n_clientes")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "pct_contacto_directo",
            F.when(
                F.col("n_visitas") > 0,
                F.col("n_contactos_directos") / F.col("n_visitas")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "avg_contactos_directos_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_contactos_directos") / F.col("n_clientes")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "pct_contacto_efectivo",
            F.when(
                F.col("n_visitas") > 0,
                F.col("n_contactos_efectivos") / F.col("n_visitas")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "avg_contactos_efectivos_x_cliente",
            F.when(
                F.col("n_clientes") > 0,
                F.col("n_contactos_efectivos") / F.col("n_clientes")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "eficiencia",
            F.when(
                F.col("pago_requerido_total") > 0,
                F.col("cobranza_total") / F.col("pago_requerido_total")
            ).otherwise(F.lit(0))
        )
        .where(
            (F.col("fisemana") >= init_week) &
            (F.col("fisemana") <= end_week)
        )
        .orderBy("fisemana")
    )

    cols_order = [
        "fisemana",
        "n_clientes",

        "n_visitas",
        "avg_visitas_x_cliente",
        
        "n_contactos_directos",
        "pct_contacto_directo",
        "avg_contactos_directos_x_cliente",
        
        "n_contactos_efectivos",
        "pct_contacto_efectivo",
        "avg_contactos_efectivos_x_cliente",
        
        "eficiencia",
        
        "cobranza_total",
        "pago_requerido_total",
    ]

    # Save csv.
    df = df.select(*cols_order)
    abs_path = os.path.abspath(f"{csv_name}.csv")
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    df.toPandas().to_csv(abs_path, index=False)