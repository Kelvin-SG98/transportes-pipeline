from pyspark.sql.functions import to_timestamp, date_format, lpad, split, concat_ws


class DateColumnsTransformer:
    """
    Classe responsável por transformar colunas de data para o formato desejado.
    """

    @staticmethod
    def transform_dates(df, date_columns, input_format, output_format="yyyy-MM-dd HH:mm"):
        """
        Transforma colunas de data para o formato desejado.
        Parâmetros:
            df: DataFrame do Spark.
            date_columns: lista de nomes das colunas de data a serem transformadas.
            input_format: formato de entrada das datas.
            output_format: formato de saída das datas (padrão é "yyyy-MM-dd HH:mm").
        """
        for col_name in date_columns:
            df = df.withColumn(
                col_name,
                date_format(
                    to_timestamp(
                        concat_ws(
                            " ",
                            split(col_name, " ")[0],
                            lpad(split(col_name, " ")[1], 5, "0"),
                        ),
                        input_format,
                    ),
                    output_format,
                ),
            )
        return df