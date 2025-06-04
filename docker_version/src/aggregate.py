from pyspark.sql.functions import col, count, avg, max, min, when
from pyspark.sql.types import IntegerType, DecimalType


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

    return df_gold.withColumn("QT_CORR_NEG", col("QT_CORR_NEG").cast(IntegerType()))\
                  .withColumn("QT_CORR_PESS", col("QT_CORR_PESS").cast(IntegerType()))\
                  .withColumn("VL_MAX_DIST", col("VL_MAX_DIST").cast(DecimalType(10, 2)))\
                  .withColumn("VL_MIN_DIST", col("VL_MIN_DIST").cast(DecimalType(10, 2)))\
                  .withColumn("VL_AVG_DIST", col("VL_AVG_DIST").cast(DecimalType(10, 2)))
