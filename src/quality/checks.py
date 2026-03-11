from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class QualityResult:
    check_name: str
    passed: bool
    failed_rows: int


def _failed_count(df: DataFrame, condition) -> int:
    return df.filter(condition).count()


def run_trip_quality_checks(df: DataFrame, config: dict) -> List[QualityResult]:
    rules = config["quality"]
    max_hours = config["processing"]["max_reasonable_trip_hours"]
    max_speed = config["processing"]["max_reasonable_trip_speed_mph"]

    checks = []
    checks.append(_evaluate("null_key_identifiers", df, F.col("pickup_location_id").isNull() | F.col("dropoff_location_id").isNull()))
    checks.append(_evaluate("payment_type_accepted", df, ~F.col("payment_type").isin(rules["accepted_payment_types"])))
    checks.append(_evaluate("rate_code_accepted", df, ~F.col("rate_code_id").isin(rules["accepted_rate_codes"])))
    checks.append(_evaluate("timestamps_valid", df, F.col("pickup_ts") >= F.col("dropoff_ts")))
    checks.append(_evaluate("non_negative_fare_distance", df, (F.col("fare_amount") < 0) | (F.col("trip_distance") < 0)))
    checks.append(_evaluate("zone_integrity", df, F.col("pickup_zone").isNull() | F.col("dropoff_zone").isNull()))
    checks.append(
        _evaluate(
            "duration_speed_anomaly",
            df,
            (F.col("trip_duration_minutes") <= 0)
            | (F.col("trip_duration_minutes") > max_hours * 60)
            | (F.col("trip_speed_mph") > max_speed),
        )
    )
    return checks


def _evaluate(name: str, df: DataFrame, condition) -> QualityResult:
    failed_rows = _failed_count(df, condition)
    return QualityResult(check_name=name, passed=failed_rows == 0, failed_rows=failed_rows)
