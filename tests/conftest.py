import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("nyc-taxi-tests").master("local[2]").getOrCreate()
    yield spark
    spark.stop()
