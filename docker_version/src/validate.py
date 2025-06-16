from pyspark.sql.functions import col

class Validator:
    """
    Classe responsável pela validação do DataFrame.
    """

    @staticmethod
    def validate_isnull(df):
        """
        Filtra o DataFrame, removendo linhas com valores nulos nas colunas essenciais.

        Parâmetros:
            df: DataFrame do Spark.

        Retorna:
            DataFrame filtrado.
        """
        return df.filter(
            col("DATA_INICIO").isNotNull()
            & col("DATA_FIM").isNotNull()
            & col("PROPOSITO").isNotNull()
        )
    
    @staticmethod
    def validate_schema(df, expected_columns):
        """
        Verifica se o DataFrame possui todas as colunas esperadas.

        Parâmetros:
            df: DataFrame do Spark.
            expected_columns: lista de nomes de colunas esperadas.

        Retorna:
            True se todas as colunas existem, False caso contrário.
        """
        df_columns = set(df.columns)
        return all(col in df_columns for col in expected_columns)