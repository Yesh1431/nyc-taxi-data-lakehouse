# Dashboard Assets

This folder contains a portfolio-ready dashboard specification built on top of the gold Delta marts.

## Dashboard: NYC Taxi Demand & Revenue Monitor

### Recommended pages
1. **Executive Overview**
   - KPI cards: total trips, total revenue, average fare, average distance
   - Daily revenue trend
   - Monthly trip trend
2. **Demand Intelligence**
   - Hourly demand heatmap by borough
   - Top zones by pickup volume
   - Peak hour distribution
3. **Revenue & Payments**
   - Revenue by borough (daily + monthly)
   - Payment type trend by time
   - Fare-per-mile and fare-per-minute by borough
4. **Airport Analytics**
   - Airport route volume (pickup/dropoff pairs)
   - Airport route revenue ranking
5. **Trip Efficiency & Anomalies**
   - Average duration and distance by borough
   - Demand spike table (z-score outliers)

## Source tables
- `data/gold/fact_taxi_trips`
- `data/gold/demand_by_zone_hour_mart`
- `data/gold/revenue_by_borough_day_mart`
- `data/gold/trip_efficiency_mart`
- `data/gold/airport_trip_mart`
- `data/gold/payment_behavior_mart`

## Tooling options
- Power BI / Tableau / Looker Studio
- Apache Superset
- Streamlit (for a Python-native dashboard)

Use `dashboards/nyc_taxi_dashboard_queries.sql` for visual-level datasets.
