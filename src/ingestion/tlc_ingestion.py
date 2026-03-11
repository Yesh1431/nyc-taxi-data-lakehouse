from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

from pyspark.sql import functions as F

from src.ingestion.download import download_file
from src.utils.logging_utils import get_logger
from src.utils.state_utils import read_processed_months, write_processed_months

logger = get_logger(__name__)


@dataclass
class IngestionResult:
    downloaded_files: List[str]
    processed_months: List[str]


def _month_range(start_month: str, end_month: str) -> List[str]:
    start = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")
    months = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


def ingest_yellow_trip_raw(config: dict, start_month: str, end_month: str, batch_id: str) -> IngestionResult:
    base_url = config["sources"]["nyc_tlc_base_url"]
    raw_path = Path(config["storage"]["raw_path"])
    state_file = config["processing"]["incremental_state_file"]
    processed = read_processed_months(state_file)

    downloaded_files, current_run_months = [], []
    for month in _month_range(start_month, end_month):
        if month in processed:
            logger.info("Skipping %s already processed", month)
            continue
        file_name = f"yellow_tripdata_{month}.parquet"
        file_path = raw_path / "yellow" / file_name
        url = f"{base_url}/{file_name}"
        download_file(url, str(file_path))
        downloaded_files.append(str(file_path))
        current_run_months.append(month)

    if current_run_months:
        write_processed_months(state_file, processed.union(current_run_months))

    return IngestionResult(downloaded_files=downloaded_files, processed_months=current_run_months)


def write_bronze_trips(spark, config: dict, file_paths: List[str], batch_id: str) -> None:
    if not file_paths:
        logger.info("No new trip files to process")
        return

    source_name = config["sources"]["source_name_trips"]
    bronze_path = f"{config['storage']['bronze_path']}/yellow_trips"

    df = spark.read.parquet(*file_paths)
    df = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_name", F.lit(source_name))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.input_file_name())
    )

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("batch_id")
        .save(bronze_path)
    )
    logger.info("Bronze yellow trips written to %s", bronze_path)


def ingest_taxi_zone_lookup(config: dict) -> str:
    destination = f"{config['storage']['raw_path']}/reference/taxi_zone_lookup.csv"
    return download_file(config["sources"]["taxi_zone_lookup_url"], destination)


def write_bronze_zones(spark, config: dict, lookup_file: str, batch_id: str) -> None:
    bronze_path = f"{config['storage']['bronze_path']}/taxi_zones"
    source_name = config["sources"]["source_name_zones"]

    df = spark.read.option("header", "true").csv(lookup_file)
    df = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_name", F.lit(source_name))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(lookup_file))
    )
    df.write.format("delta").mode("overwrite").save(bronze_path)
    logger.info("Bronze zone lookup written to %s", bronze_path)
