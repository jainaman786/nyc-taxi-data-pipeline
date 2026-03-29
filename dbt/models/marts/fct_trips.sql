{{ config(materialized='table') }}

SELECT *
FROM {{ ref('int_trips_enriched') }}
