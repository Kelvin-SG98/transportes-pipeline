from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ConsultaGold") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Caminho da tabela Gold no Delta
path_gold = "/data/gold/info_corridas_do_dia"


df_gold = spark.read.format("delta").load(path_gold)


print("==== Dados da Tabela Gold ====")
df_gold.show()


print("==== Schema da Tabela ====")
df_gold.printSchema()


df_gold.createOrReplaceTempView("info_corridas_do_dia")
resultado = spark.sql("""
    SELECT DT_REFE, QT_CORR, VL_AVG_DIST
    FROM info_corridas_do_dia
    WHERE QT_CORR > 5
""")

print("==== Resultado da Query ====")
resultado.show()

# Encerrar sessão Spark
spark.stop()
