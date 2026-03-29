{{ config(materialized='view') }}

WITH trips AS (
    SELECT * FROM {{ ref('stg_yellow_trips') }}
),

zones AS (
    SELECT * FROM {{ ref('stg_taxi_zones') }}
)

SELECT
    t.*,
    pz.zone AS pickup_zone,
    pz.borough AS pickup_borough,
    dz.zone AS dropoff_zone,
    dz.borough AS dropoff_borough
FROM trips t
LEFT JOIN zones pz ON t.pu_location_id = pz.location_id
LEFT JOIN zones dz ON t.do_location_id = dz.location_id
WHERE
    t.trip_distance > 0
    AND t.fare_amount > 0
    AND t.passenger_count > 0
    AND t.trip_duration_minutes BETWEEN 1 AND 180
