"""
Test goes here

"""
import pytest
import pandas

from mylib.lib import (
    start_spark,
    end_spark,
    extract,
    load_data,
    transform,
    query,
)

@pytest.fixture(scope="module")
def spark():
    spark = start_spark("nypd_shooting")
    yield spark
    end_spark(spark)

def test_extract():
    extracted_data = extract()
    assert extracted_data == "data/nypd_shooting.csv"


def test_load_data(spark):
    df = load_data(spark)  # The `spark` argument is provided by the fixture.
    assert df is not None


def test_transform(spark):
    df = load_data(spark)
    assert transform(df) is None


def test_query(spark):
    df = load_data(spark)
    result = query(
        spark,
        df,
        "SELECT * FROM nypd_shooting WHERE incident_key = 279473159", "nypd_shooting"
    )
    assert result is None


if __name__ == "__main__":
    test_extract()
    test_load_data(spark)
    test_transform(spark)
    test_query(spark)