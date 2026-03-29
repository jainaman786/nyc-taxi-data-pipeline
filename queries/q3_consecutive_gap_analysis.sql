/*
CONSECUTIVE TRIP GAP ANALYSIS (Q3) - Snowflake Performance Brainstormer
------------------------------------------------------------------------
Executing a `LEAD` window function grouped tightly across 38M rows per zone and day involves massive compute operations sorting over historical offsets.
To make this query lightning-fast in an Enterprise Snowflake environment:

1. Clustered Sorting: The core underlying table should be explicitly clustered by `(pickup_zone, DATE_TRUNC('day', pickup_datetime), pickup_datetime ASC)`. This sorts the data physically on disk exactly how the LEAD function intends to scan it, heavily reducing node memory shuffling.
2. Materialized Views (MVs): This aggregated mathematical gap calculation should be lifted entirely out of live ad-hoc computation. Generating a `CREATE MATERIALIZED VIEW mv_max_zone_gaps` natively caches the gap in the background upon data insertion.
3. Result Caching: In Snowflake, historical data chunks (2023 trips) are immutable. Running this query natively caches the exact output hash globally. Any subsequent BI views hitting this query execute essentially in 0.1 seconds without burning any Warehouse compute credits natively.
4. Search Optimization (SOS): We explicitly *would not* use SOS here. SOS is designed for random point lookups (e.g. finding a specific string), not for massive, sequential window aggregations.
*/

WITH gap_calculation AS (
    SELECT
        DATE_TRUNC('day', pickup_datetime) AS trip_date,
        pickup_zone,
        dropoff_datetime AS current_trip_end,
        -- The LEAD function grabs the chronologically NEXT trip's pickup time originating from the exact same zone
        LEAD(pickup_datetime) OVER (
            PARTITION BY pickup_zone, DATE_TRUNC('day', pickup_datetime) 
            ORDER BY pickup_datetime ASC
        ) AS next_trip_start
    FROM fct_trips
),

minutes_calculated AS (
    SELECT
        trip_date,
        pickup_zone,
        -- Find the gap (difference in minutes) between when this trip stopped and the next started
        DATE_DIFF('minute', current_trip_end, next_trip_start) AS gap_minutes
    FROM gap_calculation
    WHERE next_trip_start IS NOT NULL
      AND next_trip_start >= current_trip_end -- Sanity check explicitly excluding overlapping ghost trips
)

SELECT
    trip_date,
    pickup_zone,
    MAX(gap_minutes) AS max_idle_gap_minutes
FROM minutes_calculated
GROUP BY trip_date, pickup_zone
ORDER BY trip_date ASC, max_idle_gap_minutes DESC;
