{{ config(
    materialized='view'
) }}

SELECT
    'trip_started' AS event_type,
    created_at AS event_ts,
    trip_id,
    STRUCT(
        CAST(load_id AS STRING) AS load_id,
        CAST(route_id AS STRING) AS route_id,
        CAST(driver_id AS STRING) AS driver_id,
        CAST(truck_id AS STRING) AS truck_id,
        CAST(trailer_id AS STRING) AS trailer_id,
        CAST(origin_facility_id AS STRING) AS origin_facility_id,
        CAST(origin_city AS STRING) AS origin_city,
        CAST(origin_state AS STRING) AS origin_state,
        CAST(potential_revenue AS STRING) AS potential_revenue,
        CAST(estimated_arrival_at AS STRING) AS estimated_arrival_at,
        CAST(actual_arrival_at AS STRING) AS actual_arrival_at,
        CAST(pickup_detention_minutes AS STRING) AS pickup_detention_minutes,
        CAST(is_pickup_on_time AS STRING) AS is_pickup_on_time
    ) AS event_properties
FROM {{ ref('int_trip_started') }}
