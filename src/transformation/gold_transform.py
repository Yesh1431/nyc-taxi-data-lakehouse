from __future__ import annotations

from pyspark.sql import functions as F

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def build_gold_tables(spark, config: dict, run_months: list[str] | None = None) -> None:
    silver_trip_path = f"{config['storage']['silver_path']}/yellow_trips"
    silver_zone_path = f"{config['storage']['silver_path']}/taxi_zones"
    gold_root = config["storage"]["gold_path"]

    trips = spark.read.format("delta").load(silver_trip_path)
    if run_months:
        trips = trips.filter(F.col("pickup_month").isin(run_months))

    zones = spark.read.format("delta").load(silver_zone_path)
    zones.write.format("delta").mode("overwrite").save(f"{gold_root}/dim_taxi_zones")

    fact = (
        trips.withColumn("trip_date", F.to_date("pickup_ts"))
        .withColumn("trip_hour", F.hour("pickup_ts"))
        .withColumn("trip_month", F.date_format("pickup_ts", "yyyy-MM"))
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        fact.write.format("delta")
        .mode("overwrite")
        .partitionBy("trip_month")
        .save(f"{gold_root}/fact_taxi_trips")
    )

    demand = fact.groupBy("trip_date", "trip_hour", "trip_month", "pickup_location_id", "pickup_borough", "pickup_zone").agg(
        F.count("*").alias("pickup_trip_count"), F.sum("passenger_count").alias("passenger_total")
    )
    demand.write.format("delta").mode("overwrite").partitionBy("trip_month").save(
        f"{gold_root}/demand_by_zone_hour_mart"
    )

    revenue = fact.groupBy("trip_date", "trip_month", "pickup_borough").agg(
        F.sum("total_amount").alias("daily_revenue"),
        F.sum("fare_amount").alias("daily_fare"),
        F.count("*").alias("trip_count"),
    )
    revenue.write.format("delta").mode("overwrite").partitionBy("trip_month").save(
        f"{gold_root}/revenue_by_borough_day_mart"
    )

    airports = ["JFK Airport", "LaGuardia Airport", "Newark Airport"]
    airport = (
        fact.filter(F.col("pickup_zone").isin(airports) | F.col("dropoff_zone").isin(airports))
        .groupBy("trip_month", "pickup_zone", "dropoff_zone")
        .agg(F.count("*").alias("trip_count"), F.sum("total_amount").alias("revenue"))
    )
    airport.write.format("delta").mode("overwrite").partitionBy("trip_month").save(f"{gold_root}/airport_trip_mart")

    payment = fact.groupBy("trip_date", "trip_month", "pickup_borough", "payment_type").agg(
        F.count("*").alias("trip_count"), F.sum("total_amount").alias("total_revenue")
    )
    payment.write.format("delta").mode("overwrite").partitionBy("trip_month").save(
        f"{gold_root}/payment_behavior_mart"
    )

    all_fact = spark.read.format("delta").load(f"{gold_root}/fact_taxi_trips")
    efficiency = all_fact.groupBy("pickup_borough").agg(
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg(F.col("fare_amount") / F.when(F.col("trip_distance") > 0, F.col("trip_distance"))).alias("avg_fare_per_mile"),
        F.avg(F.col("fare_amount") / F.when(F.col("trip_duration_minutes") > 0, F.col("trip_duration_minutes"))).alias(
            "avg_fare_per_minute"
        ),
    )
    efficiency.write.format("delta").mode("overwrite").save(f"{gold_root}/trip_efficiency_mart")

    logger.info("Gold tables written under %s for months=%s", gold_root, run_months or "all")
