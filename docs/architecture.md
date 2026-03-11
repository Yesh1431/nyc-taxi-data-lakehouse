# Architecture Overview

## Medallion Layers
- **Raw**: downloaded monthly yellow taxi parquet files and reference CSV.
- **Bronze (Delta)**: immutable ingestion with metadata (`ingestion_timestamp`, `source_name`, `batch_id`, `source_file`).
- **Silver (Delta)**: cleaned and standardized trips, validated zones, normalized timestamps, derived duration/speed metrics.
- **Gold (Delta)**: dimensional/fact models and analytics marts for demand, revenue, efficiency, airport, and payments.

## Orchestration Flow
1. Download monthly yellow taxi parquet files incrementally based on processed-month state.
2. Download taxi zone lookup.
3. Write bronze Delta tables.
4. Build silver tables.
5. Run data quality checks.
6. Build gold tables/marts.

## Portability Notes
- Storage paths and source URLs are in `config/pipeline.yaml`.
- Spark/Delta interfaces are encapsulated in modular Python packages.
- The orchestration entrypoint can be directly adapted to Airflow/Databricks job execution.
