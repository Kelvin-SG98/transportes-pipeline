def read_csv(spark, path):
    return spark.read.csv(path, header=True, sep=";")