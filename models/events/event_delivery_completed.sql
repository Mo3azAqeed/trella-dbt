{{ config(
    materialized='view'
) }}

SELECT
    'delivery_completed' AS event_type,
    created_at AS event_ts,
    trip_id,
    STRUCT(
        CAST(load_id AS STRING) AS load_id,
        CAST(facility_id AS STRING) AS facility_id,
        CAST(delivery_city AS STRING) AS delivery_city,
        CAST(delivery_state AS STRING) AS delivery_state,
        CAST(on_time_flag AS STRING) AS on_time_flag,
        CAST(detention_mins AS STRING) AS detention_mins,
        CAST(detention_cost AS STRING) AS detention_cost,
        CAST(scheduled_delivery_at AS STRING) AS scheduled_delivery_at,
        CAST(actual_delivery_at AS STRING) AS actual_delivery_at,
        CAST(delivery_variance_hours AS STRING) AS delivery_variance_hours,
        CAST(driver_id AS STRING) AS driver_id,
        CAST(truck_id AS STRING) AS truck_id,
        CAST(trailer_id AS STRING) AS trailer_id,
        CAST(actual_distance_miles AS STRING) AS actual_distance_miles,
        CAST(actual_duration_hours AS STRING) AS actual_duration_hours,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(route_id AS STRING) AS route_id,
        CAST(potential_revenue AS STRING) AS potential_revenue,
        CAST(financial_total AS STRING) AS financial_total
    ) AS event_properties
FROM {{ ref('int_delivery_completed') }}
