from pyspark.sql import Row
from pyspark.sql import functions as F


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
