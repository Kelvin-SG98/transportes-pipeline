from pyspark.sql.functions import col, count, avg, max, min, when


def aggregate_to_gold(df_gold):
    """Agrupa e calcula as métricas diárias"""
    df_gold = df_gold.groupBy("DT_REF").agg(
            count("*").alias("QT_CORR"),
            count(when(col("CATEGORIA") == "Negocio", True)).alias("QT_CORR_NEG"),
            count(when(col("CATEGORIA") == "Pessoal", True)).alias("QT_CORR_PESS"),
            max("DISTANCIA").alias("VL_MAX_DIST"),
            min("DISTANCIA").alias("VL_MIN_DIST"),
            avg("DISTANCIA").alias("VL_AVG_DIST"),
            count(when(col("PROPOSITO") == "Reunião", True)).alias("QT_CORR_REUNI"),
            count(
                when((col("PROPOSITO").isNotNull()) & (col("PROPOSITO") != "Reunião"), True)
            ).alias("QT_CORR_NAO_REUNI")
        )
