{{ config(materialized='table') }}

SELECT
    location_id,
    Borough,
    Zone,
    service_zone
FROM {{ ref('stg_zone_lookup') }}