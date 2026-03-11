from __future__ import annotations

import argparse
from datetime import datetime

from src.ingestion.tlc_ingestion import (
    ingest_taxi_zone_lookup,
    ingest_yellow_trip_raw,
    write_bronze_trips,
    write_bronze_zones,
)
from src.quality.validator import validate_silver_trips
from src.transformation.gold_transform import build_gold_tables
from src.transformation.silver_transform import build_silver_trips, build_silver_zones
from src.utils.config import load_config
from src.utils.logging_utils import get_logger
from src.utils.spark_utils import create_spark_session

logger = get_logger(__name__)


def run_pipeline(config_path: str, start_month: str | None, end_month: str | None) -> None:
    config = load_config(config_path)
    spark = create_spark_session(config["app"]["name"])
    batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    start = start_month or config["processing"]["default_start_month"]
    end = end_month or config["processing"]["default_end_month"]

    logger.info("Running pipeline for months %s to %s", start, end)

    ingestion_result = ingest_yellow_trip_raw(config, start, end, batch_id)
    if not ingestion_result.downloaded_files:
        logger.info("No new raw files discovered for range %s..%s. Skipping trip processing.", start, end)
    else:
        write_bronze_trips(spark, config, ingestion_result.downloaded_files, batch_id)

    zone_file = ingest_taxi_zone_lookup(config)
    write_bronze_zones(spark, config, zone_file, batch_id)

    build_silver_zones(spark, config)
    if ingestion_result.processed_months:
        build_silver_trips(spark, config, run_months=ingestion_result.processed_months)
        validate_silver_trips(spark, config)
        build_gold_tables(spark, config, run_months=ingestion_result.processed_months)

    logger.info("Pipeline execution completed")
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run NYC Taxi Lakehouse pipeline")
    parser.add_argument("--config", default="config/pipeline.yaml")
    parser.add_argument("--start-month", default=None, help="YYYY-MM")
    parser.add_argument("--end-month", default=None, help="YYYY-MM")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.config, args.start_month, args.end_month)
