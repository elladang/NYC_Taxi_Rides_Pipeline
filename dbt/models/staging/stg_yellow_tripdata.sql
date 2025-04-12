SELECT
    'green' AS service_type,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationid pickup_location_id,
    dropoff_locationid dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount
FROM `trangdang.ny_taxi.yellow_tripdata` 