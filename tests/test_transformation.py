from pyspark.sql import Row
from pyspark.sql import functions as F

from src.transformation.silver_transform import apply_trip_quality_filters


def test_trip_duration_speed_calculation(spark):
    df = spark.createDataFrame(
        [Row(pickup_ts="2024-01-01 10:00:00", dropoff_ts="2024-01-01 10:30:00", trip_distance=10.0)]
    )
    calculated = (
        df.withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts", F.to_timestamp("dropoff_ts"))
        .withColumn("trip_duration_minutes", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0)
        .withColumn("trip_speed_mph", F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0))
    )
    row = calculated.collect()[0]
    assert row.trip_duration_minutes == 30.0
    assert row.trip_speed_mph == 20.0


def test_apply_trip_quality_filters_removes_invalid_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                pickup_location_id=1,
                dropoff_location_id=2,
                payment_type=1,
                rate_code_id=1,
                pickup_ts="2024-01-01 10:00:00",
                dropoff_ts="2024-01-01 10:30:00",
                fare_amount=10.0,
                trip_distance=5.0,
                pickup_zone="Midtown",
                dropoff_zone="SoHo",
                trip_duration_minutes=30.0,
                trip_speed_mph=10.0,
            ),
            Row(
                pickup_location_id=1,
                dropoff_location_id=2,
                payment_type=0,
                rate_code_id=1,
                pickup_ts="2024-01-01 11:00:00",
                dropoff_ts="2024-01-01 11:30:00",
                fare_amount=10.0,
                trip_distance=5.0,
                pickup_zone="Midtown",
                dropoff_zone="SoHo",
                trip_duration_minutes=30.0,
                trip_speed_mph=10.0,
            ),
            Row(
                pickup_location_id=1,
                dropoff_location_id=2,
                payment_type=1,
                rate_code_id=1,
                pickup_ts="2024-01-01 12:00:00",
                dropoff_ts="2024-01-01 11:00:00",
                fare_amount=10.0,
                trip_distance=5.0,
                pickup_zone="Midtown",
                dropoff_zone="SoHo",
                trip_duration_minutes=-60.0,
                trip_speed_mph=10.0,
            ),
        ]
    ).withColumn("pickup_ts", F.to_timestamp("pickup_ts")).withColumn("dropoff_ts", F.to_timestamp("dropoff_ts"))

    config = {
        "quality": {"accepted_payment_types": [1, 2], "accepted_rate_codes": [1, 2]},
        "processing": {"max_reasonable_trip_hours": 8, "max_reasonable_trip_speed_mph": 80},
    }

    cleaned = apply_trip_quality_filters(df, config)
    assert cleaned.count() == 1
