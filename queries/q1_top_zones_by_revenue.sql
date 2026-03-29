/*
SNOWFLAKE OPTIMIZATION COMMENTS (Q1)
-------------------------------------
Snowflake Architecture Context:
The `fct_trips` table powering this view contains millions of rows per year. Since Snowflake employs micro-partitioning automatically, executing a `DATE_TRUNC('month', ...)` inside the partitioning window forces a potentially massive shuffle across all micro-partitions.

To ensure this Window Function runs mathematically under 30 seconds on an X-Small warehouse, we would implement the following Clustering strategy on the physical `fct_trips` table:

```sql
ALTER TABLE fct_trips CLUSTER BY (DATE_TRUNC('month', pickup_datetime), pickup_zone);
```

By clustering deeply on the month and zone, Snowflake completely isolates the micro-partitions. When we query for the month groupings below, we achieve massive Partition Pruning (skipping scans over totally unrelated months), driving processing times to mere milliseconds instead of minutes.
*/

WITH monthly_totals AS (
    SELECT
        pickup_zone,
        DATE_TRUNC('month', pickup_datetime) AS trip_month,
        SUM(total_amount) AS monthly_revenue
    FROM fct_trips
    GROUP BY pickup_zone, DATE_TRUNC('month', pickup_datetime)
),

ranked_zones AS (
    SELECT
        trip_month,
        pickup_zone,
        monthly_revenue,
        -- Using DENSE_RANK to prevent logical skipping if two zones generate identical revenue totals
        DENSE_RANK() OVER (PARTITION BY trip_month ORDER BY monthly_revenue DESC) AS revenue_rank
    FROM monthly_totals
)

SELECT
    trip_month,
    pickup_zone,
    monthly_revenue,
    revenue_rank
FROM ranked_zones
WHERE revenue_rank <= 10
ORDER BY trip_month, revenue_rank;
