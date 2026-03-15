from __future__ import annotations

from pyspark.sql import functions as F

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def apply_trip_quality_filters(df, config: dict):
    """Apply quality-driven row filters so silver data satisfies validator rules."""
    rules = config["quality"]
    max_hours = config["processing"]["max_reasonable_trip_hours"]
    max_speed = config["processing"]["max_reasonable_trip_speed_mph"]

    return (
        df.filter(F.col("pickup_location_id").isNotNull() & F.col("dropoff_location_id").isNotNull())
        .filter(F.col("payment_type").isin(rules["accepted_payment_types"]))
        .filter(F.col("rate_code_id").isin(rules["accepted_rate_codes"]))
        .filter(F.col("pickup_ts") < F.col("dropoff_ts"))
        .filter((F.col("fare_amount") >= 0) & (F.col("trip_distance") >= 0))
        .filter(F.col("pickup_zone").isNotNull() & F.col("dropoff_zone").isNotNull())
        .filter(
            (F.col("trip_duration_minutes") > 0)
            & (F.col("trip_duration_minutes") <= max_hours * 60)
            & (F.col("trip_speed_mph") <= max_speed)
        )
    )


def build_silver_trips(spark, config: dict, run_months: list[str] | None = None) -> None:
    bronze_trip_path = f"{config['storage']['bronze_path']}/yellow_trips"
    bronze_zone_path = f"{config['storage']['bronze_path']}/taxi_zones"
    silver_path = f"{config['storage']['silver_path']}/yellow_trips"

    trips = spark.read.format("delta").load(bronze_trip_path)
    if run_months:
        trips = trips.filter(F.date_format(F.to_timestamp("tpep_pickup_datetime"), "yyyy-MM").isin(run_months))

    zones = (
        spark.read.format("delta").load(bronze_zone_path)
        .select(F.col("LocationID").cast("int").alias("location_id"), "Borough", "Zone", "service_zone")
        .dropDuplicates(["location_id"])
    )
    pu = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    do = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    silver = (
        trips.select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "Airport_fee",
            "ingestion_timestamp",
            "source_name",
            "batch_id",
            "source_file",
        )
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
        .withColumn("vendor_id", F.col("VendorID").cast("int"))
        .withColumn("rate_code_id", F.col("RatecodeID").cast("int"))
        .withColumn("pickup_location_id", F.col("PULocationID").cast("int"))
        .withColumn("dropoff_location_id", F.col("DOLocationID").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_duration_minutes", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0)
        .withColumn(
            "trip_speed_mph",
            F.when(F.col("trip_duration_minutes") > 0, F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)),
        )
        .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
        .filter(F.col("pickup_month").isNotNull())
        .dropDuplicates(
            ["pickup_ts", "dropoff_ts", "pickup_location_id", "dropoff_location_id", "fare_amount", "trip_distance"]
        )
        .join(pu, F.col("PULocationID").cast("int") == pu.pu_location_id, "left")
        .join(do, F.col("DOLocationID").cast("int") == do.do_location_id, "left")
        .drop(
            "VendorID",
            "RatecodeID",
            "PULocationID",
            "DOLocationID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "pu_location_id",
            "do_location_id",
        )
    )

    silver = apply_trip_quality_filters(silver, config)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("pickup_month")
        .save(silver_path)
    )
    logger.info("Silver trips written to %s for months=%s", silver_path, run_months or "all")


def build_silver_zones(spark, config: dict) -> None:
    bronze_zone_path = f"{config['storage']['bronze_path']}/taxi_zones"
    silver_zone_path = f"{config['storage']['silver_path']}/taxi_zones"

    zones = (
        spark.read.format("delta").load(bronze_zone_path)
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"),
        )
        .dropDuplicates(["location_id"])
    )
    zones.write.format("delta").mode("overwrite").save(silver_zone_path)
    logger.info("Silver zones written to %s", silver_zone_path)
