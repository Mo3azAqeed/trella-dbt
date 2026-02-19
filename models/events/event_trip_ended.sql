{{ config(
    materialized='view'
) }}

SELECT
    'trip_ended' AS event_type,
    created_at AS event_ts,
    trip_id,
    MAP {
        'load_id': CAST(load_id AS VARCHAR),
        'facility_id': CAST(facility_id AS VARCHAR),
        'delivery_city': CAST(delivery_city AS VARCHAR),
        'delivery_state': CAST(delivery_state AS VARCHAR),
        'on_time_flag': CAST(on_time_flag AS VARCHAR),
        'detention_minutes': CAST(detention_minutes AS VARCHAR),
        'driver_id': CAST(driver_id AS VARCHAR),
        'truck_id': CAST(truck_id AS VARCHAR),
        'trailer_id': CAST(trailer_id AS VARCHAR),
        'actual_distance_miles': CAST(actual_distance_miles AS VARCHAR),
        'actual_duration_hours': CAST(actual_duration_hours AS VARCHAR),
        'customer_id': CAST(customer_id AS VARCHAR),
        'route_id': CAST(route_id AS VARCHAR),
        'total_revenue': CAST(total_revenue AS VARCHAR),
        'fuel_cost': CAST(fuel_cost AS VARCHAR),
        'driver_cost': CAST(driver_cost AS VARCHAR),
        'detention_cost': CAST(detention_cost AS VARCHAR),
        'total_cost': CAST(total_cost AS VARCHAR),
        'total_profit': CAST(total_profit AS VARCHAR)
    } AS event_properties
FROM {{ ref('int_trip_ended') }}
