{{ config(materialized='table') }}

WITH green_trips AS (
    SELECT
        'green' AS service_type,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    FROM {{ ref('stg_green_tripdata') }}
    WHERE
        
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND pickup_datetime >= 0
        
        AND dropoff_datetime >= 0
        AND dropoff_datetime <= 253402300799999999
),
yellow_trips AS (
    SELECT
        'yellow' AS service_type,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    FROM {{ ref('stg_yellow_tripdata') }}
    WHERE
        
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND pickup_datetime >= 0
        AND pickup_datetime <= 253402300799999999
        AND dropoff_datetime >= 0
        AND dropoff_datetime <= 253402300799999999
),
combined_trips AS (
    SELECT * FROM green_trips
    UNION ALL
    SELECT * FROM yellow_trips
),
converted_trips AS (
    SELECT
        service_type,
        
        DATE(TIMESTAMP_MICROS(CAST(pickup_datetime AS INT64))) AS pickup_date,
        
        DATE(TIMESTAMP_MICROS(CAST(dropoff_datetime AS INT64))) AS dropoff_date,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    FROM combined_trips
)
SELECT
    
    ds.service_id,
    
    dd_pickup.date_id AS pickup_date_id,
    
    dd_dropoff.date_id AS dropoff_date_id,
    
    ct.pickup_location_id,
    ct.dropoff_location_id,
    
    ct.passenger_count,
    ct.trip_distance,
    ct.fare_amount,
    ct.total_amount,
    
    {{ calculate_duration('pickup_datetime', 'dropoff_datetime') }} AS trip_duration_mins
FROM converted_trips ct

LEFT JOIN {{ ref('dim_service') }} ds
    ON ct.service_type = ds.service_type

LEFT JOIN {{ ref('dim_date') }} dd_pickup
    ON ct.pickup_date = dd_pickup.date

LEFT JOIN {{ ref('dim_date') }} dd_dropoff
    ON ct.dropoff_date = dd_dropoff.date