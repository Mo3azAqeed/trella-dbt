{{ config(
    materialized='view'
) }}

SELECT
    'load_created' as event_type,
    created_at as event_ts,
    trip_id,
    STRUCT(
        CAST(load_id AS STRING) AS load_id,
        CAST(route_id AS STRING) AS route_id,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(facility_id AS STRING) AS facility_id,
        CAST(equipment_required AS STRING) AS equipment_required,
        CAST(booking_channel AS STRING) AS booking_channel,
        CAST(financial_base AS STRING) AS financial_base,
        CAST(financial_extras AS STRING) AS financial_extras,
        CAST(financial_total AS STRING) AS financial_total,
        CAST(route_distance AS STRING) AS route_distance,
        CAST(actual_rpm AS STRING) AS actual_rpm,
        CAST(benchmark_rpm AS STRING) AS benchmark_rpm,
        CAST(rpm_variance AS STRING) AS rpm_variance
    ) AS event_properties
FROM {{ ref('int_load_created') }}