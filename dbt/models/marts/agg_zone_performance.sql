/*
Brainstormer Justification (Q1):
Ranking revenue within each month (`PARTITION BY trip_month`) is vastly more useful to the business than ranking across the whole year. 
Taxi demand is highly seasonal (e.g. holidays, summer travel, winter storms). A global rank completely obscures cyclic demand rendering it useless for dynamic driver allocations. By partitioning the window function explicitly by month, operations and data analysts can spot zones that surge specifically in select months (e.g. Airport traffic in December), allowing for intelligent, historical-based workforce planning instead of a completely static, flat yearly average.
*/

{{ config(materialized='table') }}

WITH fct_trips AS (
    SELECT * FROM {{ ref('fct_trips') }}
),

monthly_zone_stats AS (
    SELECT
        DATE_TRUNC('month', pickup_datetime) AS trip_month,
        pickup_zone,
        COUNT(*) AS total_trips,
        AVG(trip_distance) AS avg_trip_distance,
        AVG(fare_amount) AS avg_fare,
        SUM(total_amount) AS total_revenue
    FROM fct_trips
    GROUP BY DATE_TRUNC('month', pickup_datetime), pickup_zone
)

SELECT
    trip_month,
    pickup_zone,
    total_trips,
    avg_trip_distance,
    avg_fare,
    total_revenue,
    RANK() OVER (PARTITION BY trip_month ORDER BY total_revenue DESC) AS revenue_rank_in_month,
    CASE 
        WHEN total_trips > 10000 THEN true 
        ELSE false 
    END AS is_high_volume_zone
FROM monthly_zone_stats
ORDER BY trip_month, revenue_rank_in_month
