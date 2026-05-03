# Cambio de departamento, por el momento solo nos quedamos con una semana
from pyspark.sql import (
        SparkSession,
        functions as F,
        DataFrame,
        Window,
        )

class CambioDeptoClasifica:
    def __init__(self,
                 spark:SparkSession,
                 params:dict,
                 )->None:
        self._spark=spark
        self._params=params

    @staticmethod
    def _get_table_name()->str:
        return "rd_baz_bdclientes.rd_clasificacteu"

    @staticmethod
    def _add_prev_week_and_prevdep(
            Data:DataFrame,
            )->DataFrame:
        window=Window.partitionBy(
                F.col("fipais"),
                F.col("ficanal"),
                F.col("fisucursal"),
                F.col("fifolio"),
                ).orderBy(
                        F.col("fisemana").asc(),
                        )
        Data=Data.withColumn(
                "fisemant",
                F.lag(
                    F.col("fisemana"),
                    1,
                    ).over(window),
                ).withColumn(
                        "fideptoant",
                        F.lag(
                            F.col("fitipodepto"),
                            1,
                            ).over(window),
                        )
        return Data


    @staticmethod
    def _final_filter(
            Data:DataFrame,
            semana:int,
            )->DataFrame:
        Data=Data.filter(
                ((F.col("fisemana")==semana) & (F.col("fitipodepto")==5) & (F.col("fideptoant")==4)),
                )
        Data=Data.withColumnRenamed(
                "fnsaldocte",
                "fnsaldototpenentradalegal",
                )
        Data=Data.withColumnRenamed(
                "fisematrasomax",
                "fisematrasentradalegal",
                ).drop(
                        F.col("fisemana"),
                        )
        return Data

    @staticmethod
    def _filter(
            Data:DataFrame,
            semana:int,
            )->DataFrame:
        window=Window.partitionBy(
                F.col("fisemana"),
                F.col("fipais"),
                F.col("ficanal"),
                F.col("fisucursal"),
                F.col("fisucursal"),
                F.col("fifolio"),
                ).orderBy(
                        F.col("fdfechasurtimiento").desc(),
                        F.col("fdfechaultimaact").desc(),
                        )
        cols=["fisemana", "fipais", "ficanal", "fisucursal", "fifolio", "fisematrasomax", "fitipodepto","fnsaldocte"]


        Data=Data.filter(
                ((F.col("fisemana").between(semana-2,semana)) & (F.col("fipais")==1) & (F.col("fitipodepto").isin([4,5]))),
                ).withColumn(
                        "rn_",
                        F.row_number().over(window),
                        ).withColumn(
                                "fisematrasomax",
                                F.when(
                                    (F.col("fisematrasomax")>0),
                                    F.col("fisematrasomax"),
                                    ).otherwise(0),
                                ).filter(
                                        (F.col("rn_")==1),
                                        ).select(*cols)
        return Data

    def get(self)->DataFrame:
        data=self._spark.table(
                self._get_table_name(),
                )
        data=self._filter(
                data,
                self._params["semana"],
                )
        data=self._add_prev_week_and_prevdep(data)
        data=self._final_filter(data,self._params["semana"])
        return data



class CambioDepto:
    def __init__(self,
                 spark:SparkSession,
                 params:dict,
                 )->None:
        self._spark=spark
        self._params=params

    @staticmethod
    def _get_table_name()->str:
        return "rd_baz_bdclientes.rd_tacteucambiodepto"

    @staticmethod
    def _get_clientes(
            Data:DataFrame,
            semana:int,
            )->DataFrame:
        cols=["fipais", "ficanal", "fisucursal", "fifolio", "fnsaldo", "fisematras", "fiperatraacum"]
        Data=Data.filter(
                ((F.col("fitipodepto")==4) & (F.col("fitipodeptocambio")==5) & (F.col("fisemana")==semana)),
                ).select(*cols)
        Data=Data.withColumnRenamed(
                "fnsaldo",
                "fnsaldototpenentradalegal",
                ).withColumnRenamed(
                        "fisematras",
                        "fisematrasentradalegal",
                        )
        return Data

    def get(self)->DataFrame:
        data=self._spark.table(
                self._get_table_name(),
                )
        data=self._get_clientes(
                data,
                self._params["semana"],
                )
        return data


class SemanasAtraso:
    def __init__(
            self,
            spark:SparkSession,
            params:dict,
            )->None:
        self._spark=spark
        self._params=params

    @staticmethod
    def _get_table_name()->str:
        return "rd_baz_bdclientes.rd_clasificacteu"

    @staticmethod
    def _filter(
            Data:DataFrame,
            semana:int,
            semanafin:int,
            )->DataFrame:
        window=Window.partitionBy(
                F.col("fisemana"),
                F.col("fipais"),
                F.col("ficanal"),
                F.col("fisucursal"),
                F.col("fifolio"),
                ).orderBy(
                        F.col("fdfechasurtimiento").desc(),
                        F.col("fdfechaultimaact").desc(),
                        )

        cols_=["fisemana", "fipais", "ficanal", "fisucursal", "fifolio", "fitipodepto", "fisematrasomax", "fnsaldocte"]
        data_=Data.withColumn(
                "rn_",
                F.row_number().over(window),
                ).filter(
                        ((F.col("fisemana").between(semana,semanafin)) & (F.col("fitipodepto").isin([5,6,15,16,28]))),
                        ).select(*cols_)
        cols_2=["fisemana", "fipais", "ficanal", "fisucursal", "fifolio", "fisematrasomax","fitipodepto"]
        data_=data_.filter(
                (F.col("rn_")==1),
                ).select(*cols_2)
        return data_

    def get(self)->DataFrame:
        data=self._spark.table(
                self._get_table_name(),
                )
        data=self._filter(
                data,
                self._params["semana"],
                self._params["semanafin"],
                )
        return data


class Pagos:
    def __init__(
            self,
            spark:SparkSession,
            params:dict,
            )->None:
        self._spark=spark
        self._params=params

    @staticmethod
    def _get_table_name()->str:
        return "rd_baz_bdclientes.rd_recuperacion_cuotas_cobranza"

    @staticmethod
    def _add_week(
            Data:DataFrame,
            )->DataFrame:
        Data=Data.withColumn(
                "num_periodo_sem",
                F.concat(
                    F.col("fianioproceso"),
                    F.lpad(
                        F.col("fisemproceso"),
                        2,
                        "0",
                        ),
                    ).cast("int"),
                )
        return Data
    @staticmethod
    def _compute_payments_by_week(
            Data:DataFrame,
            semana:int,
            semanafin:int,
            )->DataFrame:
        Data=Data.filter(
                (F.col("num_periodo_sem").between(semana,semanafin)),
                ).groupBy(
                        F.col("num_periodo_sem"),
                        F.col("fipaiscu"),
                        F.col("ficanalcu"),
                        F.col("fisucursalcu"),
                        F.col("fifoliocu"),
                        ).agg(
                                F.sum(
                                    F.when(
                                        ((F.col("fdcreccapital")+F.col("fdcrecmoratorio"))>0),
                                        F.col("fdcreccapital")+F.col("fdcrecmoratorio"),
                                        ).otherwise(F.lit(0)),
                                    ).alias("fdcrecpago"),
                                F.max(
                                    F.when(
                                        ((F.col("fdcreccapital")+F.col("fdcrecmoratorio"))>0),
                                        F.lit(1),
                                        ).otherwise(F.lit(0)),
                                    ).alias("indicadorapago"),
                                )
        return Data
    @staticmethod
    def _compute_cumsum(
            Data:DataFrame,
            )->DataFrame:
        window=Window.partitionBy(
                F.col("fipaiscu"),
                F.col("ficanalcu"),
                F.col("fisucursalcu"),
                F.col("fifoliocu"),
                ).orderBy(
                        F.col("num_periodo_sem").asc(),
                        ).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        Data=Data.withColumn(
                "fdcumsum",
                F.sum(
                    F.col("fdcrecpago"),
                    ).over(window),
                )
        return Data
    @staticmethod
    def _rename_sem(
            Data:DataFrame,
            )->DataFrame:
        Data=Data.withColumnRenamed(
                "num_periodo_sem",
                "fisemana",
                ).withColumnRenamed(
                        "fipaiscu",
                        "fipais",
                        ).withColumnRenamed(
                                "ficanalcu",
                                "ficanal",
                                ).withColumnRenamed(
                                        "fisucursalcu",
                                        "fisucursal",
                                        ).withColumnRenamed(
                                                "fifoliocu",
                                                "fifolio",
                                                )
        return Data

    def get(self)->DataFrame:
        data=self._spark.table(
                self._get_table_name(),
                )
        data=self._add_week(data)
        data=self._compute_payments_by_week(
                data,
                self._params["semana"],
                self._params["semanafin"],
                )
        data=self._compute_cumsum(data)
        data=self._rename_sem(data)
        return data


class SaldosCliente:
    def __init__(
            self,
            spark:SparkSession,
            params:dict,
            )->None:
        self._spark=spark
        self._params=params


    @staticmethod
    def _get_cs_table_name()->str:
        return "rd_baz_bdclientes.rd_cat_tiempo"

    @staticmethod
    def _get_pact_table_name()->str:
        return "cd_baz_bdclientes.cd_pedidos_liquidados_vivos_sem"

    @staticmethod
    def _return_cierre_semana(
            DataTiempo:DataFrame,
            semana:int,
            semanafin:int,
            )->DataFrame:
        DataTiempo=DataTiempo.withColumn(
                "naniosemana",
                F.concat(
                    F.col("nanio"),
                    F.lpad(
                        F.col("nsemanaekt"),
                        2,
                        "0",
                        ),
                    ).cast("int"),
                )
        DataTiempo=DataTiempo.filter(
                (F.col("naniosemana").between(semana,semanafin)),
                )
        DataTiempo=DataTiempo.groupBy(
                F.col("naniosemana"),
                ).agg(
                        F.max("nfecha").alias("UltimoDiaSemana"),
                        )
        return DataTiempo

    @staticmethod
    def _return_saldo_capital(
            Data:DataFrame,
            DataTiempo:DataFrame,
            )->DataFrame:

        Data=Data.alias("a").filter(
                (F.col("a.saldocapitalpendiente")>=0),
                ).join(
                        DataTiempo.alias("b"),
                        on=(F.col("a.fecharegistrosemanal")==F.col("b.UltimoDiaSemana")),
                        how="inner",
                        )
        Data=Data.groupBy(
                F.col("fisemana"),
                F.col("clienteunicopais"),
                F.col("clienteunicocanal"),
                F.col("clienteunicosucursal"),
                F.col("clienteunicofolio"),
                ).agg(
                        F.sum("saldototalpendiente").alias("saldototalpendiente"),
                        F.sum("saldocapitalpendiente").alias("saldocapitalpendiente"),
                        F.max("periodoatrasoacumulado").alias("periodoatrasoacumulado"),
                        )
        return Data


    @staticmethod
    def _rename_cols(
            Data:DataFrame,
            )->DataFrame:
        Data=Data.withColumnRenamed(
                "clienteunicopais",
                "fipais",
                ).withColumnRenamed(
                        "clienteunicosucursal",
                        "fisucursal",
                        ).withColumnRenamed(
                                "clienteunicofolio",
                                "fifolio",
                                ).withColumnRenamed(
                                        "clienteunicocanal",
                                        "ficanal",
                                        )
        return Data


    def get(
            self,
            )->DataFrame:
        tabla_tiempo=self._spark.table(
                self._get_cs_table_name(),
                )
        tabla_pact=self._spark.table(
                self._get_pact_table_name(),
                )
        tabla_tiempo=self._return_cierre_semana(
                tabla_tiempo,
                self._params["semana"],
                self._params["semanafin"],
                )
        data=self._return_saldo_capital(
                tabla_pact,
                tabla_tiempo,
                )
        data=self._rename_cols(
                data,
                )
        return data



class SaldoCapitalAldo:
    def __init__(
            self,
            spark:SparkSession,
            params:dict,
            )->None:
        self._spark=spark
        self._params=params

    @staticmethod
    def _get_table_name()->str:
        return "ws_ec_tmp_baz_bdclientes.tt_1019289_atraso_sdocap_historia"

    @staticmethod
    def _split_names(
            Data:DataFrame,
            )->DataFrame:
        Data=Data.withColumn(
                "tmp_",
                F.split(F.col("id_cliente"),"-"),
                ).drop(
                        F.col("id_cliente"),
                        )
        Data=Data.withColumn(
                "fipais",
                F.col("tmp_").getItem(0).cast("int"),
                ).withColumn(
                        "ficanal",
                        F.col("tmp_").getItem(1).cast("int"),
                        ).withColumn(
                                "fisucursal",
                                F.col("tmp_").getItem(2).cast("long"),
                                ).withColumn(
                                        "fifolio",
                                        F.col("tmp_").getItem(3).cast("long"),
                                        )
        Data=Data.drop("tmp_")
        return Data

    @staticmethod
    def _rename_and_filter(
            Data:DataFrame,
            semana:int,
            semanafin:int,
            )->DataFrame:
        Data=Data.filter(
                (F.col("num_periodo_sem").between(semana,semanafin)),
                ).withColumnRenamed(
                        "num_periodo_sem",
                        "fisemana",
                        ).withColumnRenamed(
                                "sld_capital_pendiente",
                                "saldocapitalpendiente",
                                ).withColumnRenamed(
                                        "num_sem_atraso",
                                        "periodoatrasoacumulado",
                                        )
        return Data

    def get(self)->DataFrame:
        data=self._spark.table(
                self._get_table_name(),
                )
        data=self._rename_and_filter(
                data,
                self._params["semana"],
                self._params["semanafin"],
                )
        data=self._split_names(data)
        return data


class Consolidado:
    def __init__(self,
                 spark:SparkSession,
                 params:dict,
                 )->None:
        self._spark=spark
        self._params=params

    @staticmethod
    def _create_client_pivot(
            Data:DataFrame,
            Data1:DataFrame,
            )->DataFrame:

        cols=[
                "fipais",
                "ficanal",
                "fisucursal",
                "fifolio",
                "fisemana",
                "fisematrasomax",
                "fnsaldototpenentradalegal",
                "fisematrasentradalegal",
                ]

        Data=Data.join(
                Data1,
                on=[
                    "fipais",
                    "ficanal",
                    "fisucursal",
                    "fifolio",
                    ],
                how="inner",
                )
        Data=Data.select(*cols)
        return Data

    @staticmethod
    def _add_payments(
            Data:DataFrame,
            Pagos:DataFrame,
            )->DataFrame:
        Data=Data.join(
                Pagos,
                on=[
                    "fisemana",
                    "fipais",
                    "ficanal",
                    "fisucursal",
                    "fifolio",
                    ],
                how="left",
                )
        return Data

    @staticmethod
    def _add_saldos(
            Data:DataFrame,
            Saldos:DataFrame,
            )->DataFrame:
        Data=Data.join(
                Saldos,
                on=[
                    "fisemana",
                    "fipais",
                    "ficanal",
                    "fisucursal",
                    "fifolio",
                    ],
                how="left",
                )
        return Data

    @staticmethod
    def _add_semama_entrada(
            Data:DataFrame,
            semana:int,
            )->DataFrame:
        Data=Data.withColumn(
                "fisemanaregistro",
                F.lit(semana),
                )
        return Data

    @staticmethod
    def _add_diferencia_semana(
            Data:DataFrame,
            )->DataFrame:
        Data=Data.withColumn(
                "fisemdif",
                ((F.col("fisemana") / 100).cast("int") * 52 + (F.col("fisemana") % 100)) -
                ((F.col("fisemanaregistro") / 100).cast("int") * 52 + (F.col("fisemanaregistro") % 100)),
                )
        return Data

    def get(
            self,
            sematraso:DataFrame,
            cambio:DataFrame,
            pagos:DataFrame,
            saldos:DataFrame,
            )->DataFrame:
        data=self._create_client_pivot(
                sematraso,
                cambio,
                )
        data=self._add_payments(data,pagos)
        data=self._add_saldos(data,saldos)
        data=self._add_semama_entrada(data,self._params["semana"])
        data=self._add_diferencia_semana(data)
        return data
