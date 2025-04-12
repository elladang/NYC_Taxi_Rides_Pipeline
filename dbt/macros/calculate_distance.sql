{% macro calculate_duration(start_time, end_time) %}
    DATE_DIFF(
        TIMESTAMP_MICROS(CAST({{ end_time }} AS INT64)),
        TIMESTAMP_MICROS(CAST({{ start_time }} AS INT64)),
        MINUTE
    )
{% endmacro %}