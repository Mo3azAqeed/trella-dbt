{{ config(
    materialized='view'
) }}

SELECT
    'fuel_purchased' AS event_type,
    created_at AS event_ts,
    trip_id,
    MAP {
        'fuel_purchase_id': CAST(fuel_purchase_id AS VARCHAR),
        'truck_id': CAST(truck_id AS VARCHAR),
        'driver_id': CAST(driver_id AS VARCHAR),
        'fuel_city': CAST(fuel_city AS VARCHAR),
        'fuel_state': CAST(fuel_state AS VARCHAR),
        'gallons': CAST(gallons AS VARCHAR),
        'price_per_gallon': CAST(price_per_gallon AS VARCHAR),
        'fuel_cost': CAST(fuel_cost AS VARCHAR),
        'fuel_card_number': CAST(fuel_card_number AS VARCHAR),
        'load_id': CAST(load_id AS VARCHAR),
        'actual_distance_miles': CAST(actual_distance_miles AS VARCHAR),
        'actual_duration_hours': CAST(actual_duration_hours AS VARCHAR),
        'customer_id': CAST(customer_id AS VARCHAR),
        'route_id': CAST(route_id AS VARCHAR)
    } AS event_properties
FROM {{ ref('int_fuel_purchased') }}
