/*
HOURLY DEMAND PATTERN (Q2)
---------------------------
Note on computations: 
Using `EXTRACT(hour ...)` gives us a clean 0-23 grouping to analyze driver efficiency dynamically.
We integrate an advanced Rolling Average window bounded tightly (`2 PRECEDING AND CURRENT ROW`) to smooth out random anomalies in ride data without losing the temporal relevance of the immediate 3-hour shift block.
*/

WITH hourly_stats AS (
    SELECT
        EXTRACT(hour FROM pickup_datetime) AS trip_hour,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS avg_fare,
        -- Calculate Tip Percentage cleanly safeguarding against massive division by zero errors
        -- COALESCE guarantees a 0 value if the entire hour is mathematically NULL
        COALESCE(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 0) AS avg_tip_percentage
    FROM fct_trips
    GROUP BY EXTRACT(hour FROM pickup_datetime)
)

SELECT
    trip_hour,
    total_trips,
    avg_fare,
    avg_tip_percentage,
    -- Execute a precise 3-hour rolling average window tracking dynamic trip demand
    AVG(total_trips) OVER (
        ORDER BY trip_hour 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3h_avg_trips
FROM hourly_stats
ORDER BY trip_hour ASC;
