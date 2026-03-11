-- 1. Hourly pickup demand by zone and borough
SELECT trip_date, trip_hour, pickup_borough, pickup_zone, pickup_trip_count
FROM demand_by_zone_hour_mart
ORDER BY trip_date, trip_hour, pickup_trip_count DESC;

-- 2. Daily and monthly revenue trends
SELECT trip_date, pickup_borough, daily_revenue
FROM revenue_by_borough_day_mart
ORDER BY trip_date, daily_revenue DESC;

SELECT DATE_TRUNC('month', trip_date) AS revenue_month,
       pickup_borough,
       SUM(daily_revenue) AS monthly_revenue
FROM revenue_by_borough_day_mart
GROUP BY 1,2
ORDER BY 1,3 DESC;

-- 3. Peak hour and peak zone analysis
SELECT pickup_zone, trip_hour, SUM(pickup_trip_count) AS hourly_demand
FROM demand_by_zone_hour_mart
GROUP BY 1,2
ORDER BY hourly_demand DESC
LIMIT 20;

-- 4. Average trip distance and duration by borough
SELECT pickup_borough, avg_trip_distance, avg_trip_duration_minutes
FROM trip_efficiency_mart
ORDER BY avg_trip_distance DESC;

-- 5. Airport trip analysis
SELECT pickup_zone, dropoff_zone, trip_count, revenue
FROM airport_trip_mart
ORDER BY trip_count DESC;

-- 6. Payment type trends over time
SELECT trip_date, pickup_borough, payment_type, trip_count, total_revenue
FROM payment_behavior_mart
ORDER BY trip_date, trip_count DESC;

-- 7. High-demand zone ranking
SELECT pickup_zone,
       SUM(pickup_trip_count) AS total_pickups,
       DENSE_RANK() OVER (ORDER BY SUM(pickup_trip_count) DESC) AS demand_rank
FROM demand_by_zone_hour_mart
GROUP BY pickup_zone
ORDER BY demand_rank
LIMIT 25;

-- 8. Zones with unusual demand spikes (z-score style)
WITH zone_daily AS (
    SELECT trip_date, pickup_zone, SUM(pickup_trip_count) AS pickups
    FROM demand_by_zone_hour_mart
    GROUP BY 1,2
),
stats AS (
    SELECT pickup_zone,
           AVG(pickups) AS avg_pickups,
           STDDEV_POP(pickups) AS std_pickups
    FROM zone_daily
    GROUP BY pickup_zone
)
SELECT z.trip_date,
       z.pickup_zone,
       z.pickups,
       (z.pickups - s.avg_pickups) / NULLIF(s.std_pickups, 0) AS z_score
FROM zone_daily z
JOIN stats s ON z.pickup_zone = s.pickup_zone
WHERE (z.pickups - s.avg_pickups) / NULLIF(s.std_pickups, 0) > 2.5
ORDER BY z_score DESC;
