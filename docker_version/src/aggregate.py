from pyspark.sql.functions import col, count, avg


def generate_aggregates(df):
    return df.groupBy("MOTIVO").agg(
        count("*").alias("qtde"), avg("DURACAO").alias("media_duracao")
    )
