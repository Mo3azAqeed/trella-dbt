{{ config(
    materialized='view'
) }}

SELECT
    'fuel_purchased' AS event_type,
    created_at AS event_ts,
    trip_id,
    STRUCT(
        CAST(fuel_purchase_id AS STRING) AS fuel_purchase_id,
        CAST(truck_id AS STRING) AS truck_id,
        CAST(driver_id AS STRING) AS driver_id,
        CAST(fuel_city AS STRING) AS fuel_city,
        CAST(fuel_state AS STRING) AS fuel_state,
        CAST(gallons AS STRING) AS gallons,
        CAST(price_per_gallon AS STRING) AS price_per_gallon,
        CAST(fuel_cost AS STRING) AS fuel_cost,
        CAST(fuel_card_number AS STRING) AS fuel_card_number,
        CAST(load_id AS STRING) AS load_id,
        CAST(actual_distance_miles AS STRING) AS actual_distance_miles,
        CAST(actual_duration_hours AS STRING) AS actual_duration_hours,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(route_id AS STRING) AS route_id
    ) AS event_properties
FROM {{ ref('int_fuel_purchased') }}
