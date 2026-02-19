{{ config(
    materialized='view'
) }}

SELECT
    'safety_incident' AS event_type,
    created_at AS event_ts,
    trip_id,
    STRUCT(
        CAST(incident_id AS STRING) AS incident_id,
        CAST(incident_type AS STRING) AS incident_type,
        CAST(incident_city AS STRING) AS incident_city,
        CAST(incident_state AS STRING) AS incident_state,
        CAST(truck_id AS STRING) AS truck_id,
        CAST(driver_id AS STRING) AS driver_id,
        CAST(at_fault_flag AS STRING) AS at_fault_flag,
        CAST(injury_flag AS STRING) AS injury_flag,
        CAST(preventable_flag AS STRING) AS preventable_flag,
        CAST(vehicle_damage_cost AS STRING) AS vehicle_damage_cost,
        CAST(cargo_damage_cost AS STRING) AS cargo_damage_cost,
        CAST(total_damage_cost AS STRING) AS total_damage_cost,
        CAST(claim_amount AS STRING) AS claim_amount,
        CAST(load_id AS STRING) AS load_id,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(route_id AS STRING) AS route_id,
        CAST(trip_total_revenue AS STRING) AS trip_total_revenue,
        CAST(revenue_after_damage AS STRING) AS revenue_after_damage,
        CAST(is_trip_still_profitable AS STRING) AS is_trip_still_profitable
    ) AS event_properties
FROM {{ ref('int_safety_incident') }}
