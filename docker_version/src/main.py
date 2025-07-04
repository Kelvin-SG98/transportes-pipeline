import os
from pyspark.sql import SparkSession
from src.ingest import Ingest
from src.transform_facada import Transformer
from src.aggregate import Aggregator
from src.save import Write

class TransportePipeline:
    def __init__(self, spark, input_path, expected_columns, data_format, ref_format, date_columns, agg_column):
        self.spark = spark
        self.input_path = input_path
        self.expected_columns = expected_columns
        self.data_format = data_format
        self.ref_format = ref_format
        self.date_columns = date_columns
        self.agg_column = agg_column
        self.transformer = Transformer(data_format, ref_format, date_columns, agg_column)

    def run(self):
        df_bronze = Ingest.read_csv(self.spark, self.input_path)
        Write.write_delta(df_bronze, "/data/bronze", "bronze", "info_transportes_raw")
        df_silver = self.transformer.transform(df_bronze)
        Write.write_delta(df_silver, "/data/silver", "silver", "info_transportes_clean")
        df_gold = Aggregator.aggregate_to_gold(df_silver, self.agg_column)
        Write.write_delta(df_gold, "/data/gold", "gold", "info_corridas_do_dia")
        return df_gold

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("TransportePipeline")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .getOrCreate()
            )
    

    expected_columns = os.getenv("EXPECTED_COLUMNS", "")
    expected_columns = [col.strip() for col in expected_columns.split(",") if col.strip()]
    input_path = "data/info_transportes.csv"
    data_format = os.getenv("EXPECTED_DATA_FORMAT", "MM-dd-yyyy HH:mm")
    ref_format = os.getenv("EXPECTED_DEFAULT_DATA_REF", "yyyy-MM-dd")
    date_columns = os.getenv("DATE_COLUMNS", "")
    date_columns = [col.strip() for col in date_columns.split(",") if col.strip()]
    agg_column = os.getenv("AGG_COLUMN", "DT_REF")

    pipeline = TransportePipeline(spark, input_path, expected_columns, data_format, ref_format, date_columns, agg_column)
    df_gold = pipeline.run()
    df_gold.show()