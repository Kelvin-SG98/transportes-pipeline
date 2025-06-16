class Ingest:
    """
    Classe responsável pela ingestão de dados no pipeline de transportes.
    """

    @staticmethod
    def read_csv(spark, path):
        """
        Lê um arquivo CSV usando Spark.

        Parâmetros:
            spark: SparkSession ativa.
            path: Caminho para o arquivo CSV.

        Retorna:
            DataFrame do Spark com os dados lidos.
        """
        return spark.read.csv(path, header=True, sep=";")