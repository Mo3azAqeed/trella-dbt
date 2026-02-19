{{ config(
    materialized='view'
) }}

SELECT
    'trip_ended' AS event_type,
    created_at AS event_ts,
    trip_id,
    STRUCT(
        CAST(load_id AS STRING) AS load_id,
        CAST(facility_id AS STRING) AS facility_id,
        CAST(delivery_city AS STRING) AS delivery_city,
        CAST(delivery_state AS STRING) AS delivery_state,
        CAST(on_time_flag AS STRING) AS on_time_flag,
        CAST(detention_minutes AS STRING) AS detention_minutes,
        CAST(driver_id AS STRING) AS driver_id,
        CAST(truck_id AS STRING) AS truck_id,
        CAST(trailer_id AS STRING) AS trailer_id,
        CAST(actual_distance_miles AS STRING) AS actual_distance_miles,
        CAST(actual_duration_hours AS STRING) AS actual_duration_hours,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(route_id AS STRING) AS route_id,
        CAST(total_revenue AS STRING) AS total_revenue,
        CAST(fuel_cost AS STRING) AS fuel_cost,
        CAST(driver_cost AS STRING) AS driver_cost,
        CAST(detention_cost AS STRING) AS detention_cost,
        CAST(total_cost AS STRING) AS total_cost,
        CAST(total_profit AS STRING) AS total_profit
    ) AS event_properties
FROM {{ ref('int_trip_ended') }}
