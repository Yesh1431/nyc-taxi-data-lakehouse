# nyc-taxi-demand-revenue-lakehouse

Production-style end-to-end data engineering project that ingests NYC TLC yellow taxi trip records and publishes analytics-ready Delta Lake tables for demand, revenue, efficiency, airport routing, and payment behavior.

## Why this project
City mobility and operations teams need consistent, trusted metrics around taxi demand and revenue by zone and borough. This repository demonstrates a practical local lakehouse implementation with modular ingestion, transformations, quality checks, orchestration, testing, and CI.

## Tech stack
- Python 3.13.9
- PySpark 3.5
- Delta Lake (`delta-spark`)
- SQL analytics
- PyTest
- Docker + GitHub Actions

## Repository layout
- `src/ingestion` - data download + bronze writes
- `src/transformation` - silver and gold transformations
- `src/quality` - data quality rules and validators
- `src/orchestration` - runnable pipeline entrypoint
- `src/utils` - config/logging/spark/state helpers
- `sql/` - business reporting SQL queries
- `tests/` - unit tests for ingestion, quality, and transformation logic
- `docs/` - architecture documentation
- `notebooks/` - Jupyter quickstart and interactive exploration
- `data/{raw,bronze,silver,gold}` - local storage layers

## Data sources
1. NYC TLC trip records: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Taxi zone lookup table

## How to run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.orchestration.pipeline --start-month 2024-01 --end-month 2024-03
```


## Run in Jupyter Notebook
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook
```

Then open:
- `notebooks/nyc_taxi_lakehouse_quickstart.ipynb`

Notebook flow:
1. Install project dependencies inside the kernel
2. Run `run_pipeline(...)` for the selected month range
3. Read Delta gold marts directly with Spark for exploration

## Run tests
```bash
pytest -q
```

## Docker run
```bash
docker compose up --build
```

## Data model outputs
### Core curated tables
- `dim_taxi_zones`
- `fact_taxi_trips`

### Business marts
- `demand_by_zone_hour_mart`
- `revenue_by_borough_day_mart`
- `trip_efficiency_mart`
- `airport_trip_mart`
- `payment_behavior_mart`

## Quality checks included
- Null checks on pickup/dropoff zone keys
- Accepted value checks for payment and rate code
- Pickup/dropoff timestamp validity
- Non-negative fare and distance validation
- Zone enrichment integrity checks
- Duration/speed anomaly checks

## Business questions supported
- Which zones have the highest pickup demand by hour?
- Which borough generates the most revenue?
- How do trip duration and trip distance vary by borough?
- Which payment methods are most common by area and time?
- What are the busiest airport routes?
- Which zones show unusual demand spikes?

## Notes for cloud adaptation
The codebase is config-driven and layered, so the same patterns map cleanly to Databricks jobs, ADLS/S3 object storage, external metastore tables, and orchestration frameworks like Airflow.

## Dashboard assets
- `dashboards/README.md` for dashboard page design and metric mapping
- `dashboards/nyc_taxi_dashboard_queries.sql` for dashboard-ready datasets
