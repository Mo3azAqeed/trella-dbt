{{ config(
    materialized='view'
) }}

SELECT
    'trip_started' AS event_type,
    created_at AS event_ts,
    'trip_id', trip_id,
    MAP {
        'load_id': CAST(load_id AS VARCHAR),
        'route_id': CAST(route_id AS VARCHAR),
        'driver_id': CAST(driver_id AS VARCHAR),
        'truck_id': CAST(truck_id AS VARCHAR),
        'trailer_id': CAST(trailer_id AS VARCHAR),
        'origin_facility_id': CAST(origin_facility_id AS VARCHAR),
        'origin_city': CAST(origin_city AS VARCHAR),
        'origin_state': CAST(origin_state AS VARCHAR),
        'potential_revenue': CAST(potential_revenue AS VARCHAR),
        'estimated_arrival_at': CAST(estimated_arrival_at AS VARCHAR),
        'actual_arrival_at': CAST(actual_arrival_at AS VARCHAR),
        'pickup_detention_minutes': CAST(pickup_detention_minutes AS VARCHAR),
        'is_pickup_on_time': CAST(is_pickup_on_time AS VARCHAR)
    } AS event_properties
FROM {{ ref('int_trip_started') }}
