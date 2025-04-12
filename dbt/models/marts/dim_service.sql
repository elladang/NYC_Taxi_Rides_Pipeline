{{ config(materialized='table') }}

SELECT
    1 AS service_id,
    'green' AS service_type
UNION ALL
SELECT
    2 AS service_id,
    'yellow' AS service_type