{{ config(materialized='table') }}

SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM {{ ref('stg_taxi_zones') }}
