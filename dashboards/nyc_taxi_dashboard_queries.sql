-- Dashboard dataset: executive KPI snapshot
SELECT
  COUNT(*) AS total_trips,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(trip_distance), 2) AS avg_trip_distance
FROM fact_taxi_trips;

-- Daily revenue trend
SELECT trip_date, pickup_borough, daily_revenue
FROM revenue_by_borough_day_mart
ORDER BY trip_date;

-- Monthly trip trend
SELECT DATE_TRUNC('month', trip_date) AS month, COUNT(*) AS trip_count
FROM fact_taxi_trips
GROUP BY 1
ORDER BY 1;

-- Hourly demand heatmap input
SELECT trip_date, trip_hour, pickup_borough, SUM(pickup_trip_count) AS trips
FROM demand_by_zone_hour_mart
GROUP BY 1,2,3
ORDER BY 1,2;

-- Top zones by demand
SELECT pickup_zone, SUM(pickup_trip_count) AS total_pickups
FROM demand_by_zone_hour_mart
GROUP BY pickup_zone
ORDER BY total_pickups DESC
LIMIT 25;

-- Payment mix trend
SELECT trip_date, pickup_borough, payment_type, trip_count, total_revenue
FROM payment_behavior_mart
ORDER BY trip_date;

-- Airport route analysis
SELECT pickup_zone, dropoff_zone, trip_count, revenue
FROM airport_trip_mart
ORDER BY trip_count DESC;

-- Trip efficiency by borough
SELECT pickup_borough, avg_trip_distance, avg_trip_duration_minutes, avg_fare_per_mile, avg_fare_per_minute
FROM trip_efficiency_mart
ORDER BY avg_fare_per_mile DESC;
