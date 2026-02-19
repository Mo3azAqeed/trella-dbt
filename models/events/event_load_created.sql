{{ config(
    materialized='view'
) }}

SELECT
    'load_created' as event_type,
    created_at as event_ts,
    'trip_id', trip_id,
    MAP {
        'load_id': CAST(load_id AS VARCHAR),
        'route_id': CAST(route_id AS VARCHAR),
        'customer_id': CAST(customer_id AS VARCHAR),
        'facility_id': CAST(facility_id AS VARCHAR),
        'equipment_required': CAST(equipment_required AS VARCHAR),
        'booking_channel': CAST(booking_channel AS VARCHAR),
        'financial_base': CAST(financial_base AS VARCHAR),
        'financial_extras': CAST(financial_extras AS VARCHAR),
        'financial_total': CAST(financial_total AS VARCHAR),
        'route_distance': CAST(route_distance AS VARCHAR),
        'actual_rpm': CAST(actual_rpm AS VARCHAR),
        'benchmark_rpm': CAST(benchmark_rpm AS VARCHAR),
        'rpm_variance': CAST(rpm_variance AS VARCHAR)
    } AS event_properties
FROM {{ ref('int_load_created') }}