from pyspark.sql.functions import to_date

class RefColumnCreator:
    """
    Classe responsável por criar a coluna de referência no formato desejado.
    """
    @staticmethod
    def create_ref_column(df, ref_col, ref_format, agg_column):
        """
        Cria uma nova coluna de referência a partir de uma coluna existente.

        Parâmetros:
            df: DataFrame do Spark.
            ref_col: nome da coluna de referência.
            ref_format: formato da data de referência.
            agg_column: nome da nova coluna de referência.

        Retorna:
            DataFrame com a nova coluna de referência.
        """
        return df.withColumn(agg_column, to_date(df[ref_col], ref_format))