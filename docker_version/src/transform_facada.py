from src.date_transform import DateColumnsTransformer
from src.create_ref import RefColumnCreator

class Transformer:
    """
    Classe orquestradora das transformações.
    """
    def __init__(self, data_format, ref_format, date_columns, agg_column):
        self.data_format = data_format
        self.ref_format = ref_format
        self.date_columns = date_columns
        self.agg_column = agg_column

    def transform(self, df_bronze, ref_col="DATA_INICIO"):
        df_silver = DateColumnsTransformer.transform_dates(df_bronze, self.date_columns, self.data_format)
        df_silver = RefColumnCreator.create_ref_column(df_silver, ref_col, self.ref_format, self.agg_column)
        return df_silver