{{ config(
    materialized='view'
) }}

SELECT
    'delivery_completed' AS event_type,
    created_at AS event_ts,
    'trip_id', trip_id,
    MAP {
        'load_id': CAST(load_id AS VARCHAR),
        'facility_id': CAST(facility_id AS VARCHAR),
        'delivery_city': CAST(delivery_city AS VARCHAR),
        'delivery_state': CAST(delivery_state AS VARCHAR),
        'on_time_flag': CAST(on_time_flag AS VARCHAR),
        'detention_mins': CAST(detention_mins AS VARCHAR),
        'detention_cost': CAST(detention_cost AS VARCHAR),
        'scheduled_delivery_at': CAST(scheduled_delivery_at AS VARCHAR),
        'actual_delivery_at': CAST(actual_delivery_at AS VARCHAR),
        'delivery_variance_hours': CAST(delivery_variance_hours AS VARCHAR),
        'driver_id': CAST(driver_id AS VARCHAR),
        'truck_id': CAST(truck_id AS VARCHAR),
        'trailer_id': CAST(trailer_id AS VARCHAR),
        'actual_distance_miles': CAST(actual_distance_miles AS VARCHAR),
        'actual_duration_hours': CAST(actual_duration_hours AS VARCHAR),
        'customer_id': CAST(customer_id AS VARCHAR),
        'route_id': CAST(route_id AS VARCHAR),
        'potential_revenue': CAST(potential_revenue AS VARCHAR),
        'financial_total': CAST(financial_total AS VARCHAR)
    } AS event_properties
FROM {{ ref('int_delivery_completed') }}
