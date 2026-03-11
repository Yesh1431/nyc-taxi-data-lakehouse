import pytest

from src.utils.spark_utils import create_spark_session


@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session("nyc-taxi-tests")
    yield spark
    spark.stop()
