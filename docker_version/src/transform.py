from pyspark.sql.functions import to_timestamp, date_format, lpad, split, concat_ws

def transform_dates(df):
    df = df.withColumn("DATA_INICIO", date_format(to_timestamp(
        concat_ws(" ", split("DATA_INICIO", " ")[0],
                      lpad(split("DATA_INICIO", " ")[1], 5, "0")), "MM-dd-yyyy HH:mm"), "yyyy-MM-dd HH:mm"))
    df = df.withColumn("DATA_FIM", date_format(to_timestamp(
        concat_ws(" ", split("DATA_FIM", " ")[0],
                      lpad(split("DATA_FIM", " ")[1], 5, "0")), "MM-dd-yyyy HH:mm"), "yyyy-MM-dd HH:mm"))
    df = df.withColumn("DT_REF", df["DATA_INICIO"])
    return df