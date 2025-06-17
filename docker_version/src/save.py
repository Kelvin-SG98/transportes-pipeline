class Write:
    """
    Classe utilitária para persistência de DataFrames em formato Delta Lake.
    """

    @staticmethod
    def write_delta(df, path, camada, table_name, mode="overwrite"):
        """
        Salva um DataFrame do Spark como uma tabela Delta em um catálogo específico, utilizando um caminho customizado.
        Se a camada não existir, ela será criada automaticamente.

        Parâmetros:
            df (pyspark.sql.DataFrame): DataFrame do Spark a ser salvo.
            path (str): Caminho no sistema de arquivos onde os dados Delta serão armazenados.
            camada (str): Nome do catálogo ou schema onde a tabela será registrada.
            table_name (str): Nome da tabela a ser criada ou sobrescrita.
            mode (str, opcional): Modo de escrita (por padrão "overwrite"). Pode ser "append", "overwrite", "ignore" ou "error".

        A função grava os dados no formato Delta no caminho especificado e registra a tabela no catálogo do Spark sob o nome camada.table_name.
        """
        df.sparkSession.sql(f"CREATE DATABASE IF NOT EXISTS {camada}")

        df.write.format("delta").mode(mode).option("path", path).saveAsTable(f"{camada}.{table_name}")