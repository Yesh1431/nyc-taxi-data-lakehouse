from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def is_delta_session_configured(spark: SparkSession) -> bool:
    """Return True when the running Spark session has Delta SQL configs set."""
    try:
        ext = spark.conf.get("spark.sql.extensions", "")
        catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")
    except Exception:
        return False
    return (DELTA_EXTENSION in ext) and (catalog == DELTA_CATALOG)


def _delta_builder(app_name: str):
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )


def ensure_delta_spark_session(app_name: str, stop_existing_non_delta: bool = True) -> SparkSession:
    """Get a Delta-enabled Spark session, rebuilding active sessions that are missing Delta config.

    This is especially useful in notebooks where a plain Spark session may already be active.
    """
    active = SparkSession.getActiveSession()
    if active and is_delta_session_configured(active):
        return active

    if active and stop_existing_non_delta:
        active.stop()

    return configure_spark_with_delta_pip(_delta_builder(app_name)).getOrCreate()


def create_spark_session(app_name: str) -> SparkSession:
    return ensure_delta_spark_session(app_name)
