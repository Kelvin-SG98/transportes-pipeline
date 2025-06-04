from pyspark.sql.functions import col


def validate_schema(df):
    return df.filter(
        col("DATA_INICIO").isNotNull()
        & col("DATA_FIM").isNotNull()
        & col("MOTIVO").isNotNull()
    )
