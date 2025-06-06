from src.ingest import read_csv
from src.validate import validate_schema
from src.transform import transform_dates
from src.aggregate import aggregate_to_gold
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransportePipeline").getOrCreate()

    df = read_csv(spark, "data/info_transportes.csv")
    df = validate_schema(df)
    df = transform_dates(df)
    df_gold = aggregate_to_gold(df)

    df_gold.show()