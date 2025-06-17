import pytest
from pyspark.sql import SparkSession
from src.ingest import Ingest
from src.date_transform import DateColumnsTransformer
from src.create_ref import RefColumnCreator
from src.transform_facada import Transformer
from src.aggregate import Aggregator


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("TransportePipelineTests").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    yield spark
    spark.stop()


def test_read_csv(spark):
    input_path = "data/sample.csv"
    df_bronze = Ingest.read_csv(spark, input_path)

    assert df_bronze is not None
    assert df_bronze.count() > 0
    assert df_bronze.columns == [
        "id_corrida",
        "distancia",
        "categoria",
        "proposito",
        "dt_corrida",
    ]


def test_transform_dates(spark):
    data = [("01-01-2016 21:11", "01-01-2016 21:17")]
    columns = ["DATA_INICIO", "DATA_FIM"]
    df = spark.createDataFrame(data, columns)

    data_format = "MM-dd-yyyy HH:mm"
    date_columns = ["DATA_INICIO", "DATA_FIM"]

    df_transformed = DateColumnsTransformer.transform_dates(
        df, date_columns, data_format
    )

    row = df_transformed.first()
    assert row["DATA_INICIO"].count("-") == 2
    assert len(row["DATA_INICIO"]) == 16  # yyyy-MM-dd HH:mm


def test_create_ref_column(spark):
    data = [("2024-06-25 10:00",)]
    columns = ["DATA_INICIO"]
    df = spark.createDataFrame(data, columns)

    ref_format = "yyyy-MM-dd"
    df_ref = RefColumnCreator.create_ref_column(df, "DATA_INICIO", ref_format, "DT_REF")

    row = df_ref.first()
    assert "DT_REF" in df_ref.columns
    assert row["DT_REF"].strftime("%Y-%m-%d") == "2024-06-25"


def test_transform(spark):
    data = [
        ("01-01-2016 21:11", "01-01-2016 21:17", "Negocio", "Reunião"),
        ("01-02-2016 10:00", "01-02-2016 10:30", "Pessoal", "Outro"),
    ]
    columns = ["DATA_INICIO", "DATA_FIM", "CATEGORIA", "PROPOSITO"]
    df = spark.createDataFrame(data, columns)

    data_format = "MM-dd-yyyy HH:mm"
    ref_format = "yyyy-MM-dd"
    date_columns = ["DATA_INICIO", "DATA_FIM"]
    agg_column = "DT_REF"

    transformer = Transformer(data_format, ref_format, date_columns, agg_column)
    df_transformed = transformer.transform(df)

    assert df_transformed is not None
    assert df_transformed.count() > 0


def test_aggregate(spark):
    data = [
        ("2024-06-25", "Negocio", 10.0, "Reunião"),
        ("2024-06-25", "Negocio", 20.0, "Outro"),
        ("2024-06-25", "Pessoal", 30.0, "Reunião"),
        ("2024-06-26", "Negocio", 40.0, "Outro"),
    ]
    columns = ["DT_REF", "CATEGORIA", "DISTANCIA", "PROPOSITO"]
    df = spark.createDataFrame(data, columns)

    # Executa a agregação
    df_gold = Aggregator.aggregate_to_gold(df, "DT_REF")

    # Coleta os resultados para checagem
    results = {row["DT_REF"]: row.asDict() for row in df_gold.collect()}

    # Verifica se as colunas agregadas existem
    for col in [
        "QT_CORR",
        "QT_CORR_NEG",
        "QT_CORR_PESS",
        "VL_MAX_DIST",
        "VL_MIN_DIST",
        "VL_AVG_DIST",
        "QT_CORR_REUNI",
        "QT_CORR_NAO_REUNI",
    ]:
        assert col in df_gold.columns

    # Verifica valores agregados para 2024-06-25
    row = results["2024-06-25"]
    assert row["QT_CORR"] == 3
    assert row["QT_CORR_NEG"] == 2
    assert row["QT_CORR_PESS"] == 1
    assert row["VL_MAX_DIST"] == 30.00
    assert row["VL_MIN_DIST"] == 10.00
    assert round(row["VL_AVG_DIST"], 2) == 20.00
    assert row["QT_CORR_REUNI"] == 2
    assert row["QT_CORR_NAO_REUNI"] == 1

