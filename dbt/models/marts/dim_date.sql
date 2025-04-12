{{ config(materialized='table') }}

WITH date_series AS (
    SELECT
        date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE('2020-01-01'),  -- Ngày bắt đầu
            DATE('2025-12-31'),  -- Ngày kết thúc
            INTERVAL 1 DAY        -- Khoảng cách giữa các ngày
        )
    ) AS date
)
SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_id,
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    CASE EXTRACT(DAYOFWEEK FROM date)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_of_week
FROM date_series
ORDER BY date