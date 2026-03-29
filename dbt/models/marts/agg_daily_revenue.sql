{{ config(materialized='table') }}

WITH fct_trips AS (
    SELECT * FROM {{ ref('fct_trips') }}
)

SELECT
    DATE_TRUNC('day', pickup_datetime) AS pickup_date,
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare,
    SUM(tip_amount) AS total_tips,
    -- Utilizing NULLIF explicitly to bypass unhandled division by zero
    COALESCE((SUM(tip_amount) / NULLIF(SUM(fare_amount), 0)) * 100, 0) AS tip_rate_percent
FROM fct_trips
GROUP BY DATE_TRUNC('day', pickup_datetime)
ORDER BY pickup_date
