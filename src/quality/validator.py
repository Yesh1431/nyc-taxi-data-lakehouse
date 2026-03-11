from src.quality.checks import run_trip_quality_checks
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def validate_silver_trips(spark, config: dict) -> None:
    silver_path = f"{config['storage']['silver_path']}/yellow_trips"
    df = spark.read.format("delta").load(silver_path)
    results = run_trip_quality_checks(df, config)

    failed = [r for r in results if not r.passed]
    for result in results:
        logger.info("DQ check=%s passed=%s failed_rows=%s", result.check_name, result.passed, result.failed_rows)

    if failed:
        raise ValueError(f"Data quality validation failed: {[f.check_name for f in failed]}")
