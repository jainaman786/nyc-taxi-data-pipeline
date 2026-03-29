{{ config(materialized='view') }}

select
    -- Identifiers
    "VendorID" as vendor_id,
    "PULocationID" as pu_location_id,
    "DOLocationID" as do_location_id,
    "RatecodeID" as ratecode_id,
    
    -- Timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) as pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) as dropoff_datetime,
    
    -- Derived duration
    DATE_DIFF('minute', CAST(tpep_pickup_datetime AS TIMESTAMP), CAST(tpep_dropoff_datetime AS TIMESTAMP)) as trip_duration_minutes,
    
    -- Trip info
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    
    -- Payment info
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee as airport_fee

from {{ source('nyc_tlc', 'yellow_trips') }}
