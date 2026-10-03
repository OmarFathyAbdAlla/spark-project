import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield session
    session.stop()


def make_df(spark, rows):
    return spark.createDataFrame(rows, "name string, amount double")


def test_valid_records_are_kept(spark):
    df = make_df(spark, [("Ali", 100.0), ("Sara", 50.0)])
    assert clean_data(df).count() == 2


def test_amount_less_or_equal_zero_removed(spark):
    df = make_df(spark, [("Ali", 100.0), ("Zero", 0.0), ("Neg", -5.0)])
    names = [r["name"] for r in clean_data(df).collect()]
    assert names == ["Ali"]


def test_null_names_removed(spark):
    df = make_df(spark, [("Ali", 100.0), (None, 80.0)])
    result = clean_data(df)
    assert result.count() == 1
    assert result.collect()[0]["name"] == "Ali"


def test_amount_with_tax_calculated_correctly(spark):
    df = make_df(spark, [("Ali", 100.0)])
    row = clean_data(df).collect()[0]
    assert row["amount_with_tax"] == pytest.approx(120.0)
