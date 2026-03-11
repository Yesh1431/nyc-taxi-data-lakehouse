from pyspark.sql import Row

from src.quality.checks import run_trip_quality_checks


def test_quality_checks_detect_bad_rows(spark):
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
        ]
    )
    config = {
        "quality": {"accepted_payment_types": [1, 2], "accepted_rate_codes": [1, 2]},
        "processing": {"max_reasonable_trip_hours": 8, "max_reasonable_trip_speed_mph": 80},
    }
    results = run_trip_quality_checks(df, config)
    assert any(not result.passed for result in results)
