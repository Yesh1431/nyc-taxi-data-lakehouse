from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.quality.checks import run_trip_quality_checks


def test_quality_checks_detect_bad_rows(spark):
    schema = StructType(
        [
            StructField("pickup_location_id", IntegerType(), True),
            StructField("dropoff_location_id", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("rate_code_id", IntegerType(), True),
            StructField("pickup_ts", StringType(), True),
            StructField("dropoff_ts", StringType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_zone", StringType(), True),
            StructField("dropoff_zone", StringType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
            StructField("trip_speed_mph", DoubleType(), True),
        ]
    )

    df = spark.createDataFrame(
        [
            Row(
                pickup_location_id=None,
                dropoff_location_id=2,
                payment_type=1,
                rate_code_id=1,
                pickup_ts="2024-01-01 10:00:00",
                dropoff_ts="2024-01-01 09:00:00",
                fare_amount=-1.0,
                trip_distance=-2.0,
                pickup_zone=None,
                dropoff_zone="Midtown",
                trip_duration_minutes=-5.0,
                trip_speed_mph=120.0,
            )
        ],
        schema=schema,
    )
    config = {
        "quality": {"accepted_payment_types": [1, 2], "accepted_rate_codes": [1, 2]},
        "processing": {"max_reasonable_trip_hours": 8, "max_reasonable_trip_speed_mph": 80},
    }
    results = run_trip_quality_checks(df, config)
    assert any(not result.passed for result in results)
