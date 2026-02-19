{{ config(
    materialized='view'
) }}

SELECT
    'safety_incident' AS event_type,
    created_at AS event_ts,
    trip_id,
    MAP {
        'incident_id': CAST(incident_id AS VARCHAR),
        'incident_type': CAST(incident_type AS VARCHAR),
        'incident_city': CAST(incident_city AS VARCHAR),
        'incident_state': CAST(incident_state AS VARCHAR),
        'truck_id': CAST(truck_id AS VARCHAR),
        'driver_id': CAST(driver_id AS VARCHAR),
        'at_fault_flag': CAST(at_fault_flag AS VARCHAR),
        'injury_flag': CAST(injury_flag AS VARCHAR),
        'preventable_flag': CAST(preventable_flag AS VARCHAR),
        'vehicle_damage_cost': CAST(vehicle_damage_cost AS VARCHAR),
        'cargo_damage_cost': CAST(cargo_damage_cost AS VARCHAR),
        'total_damage_cost': CAST(total_damage_cost AS VARCHAR),
        'claim_amount': CAST(claim_amount AS VARCHAR),
        'load_id': CAST(load_id AS VARCHAR),
        'customer_id': CAST(customer_id AS VARCHAR),
        'route_id': CAST(route_id AS VARCHAR),
        'trip_total_revenue': CAST(trip_total_revenue AS VARCHAR),
        'revenue_after_damage': CAST(revenue_after_damage AS VARCHAR),
        'is_trip_still_profitable': CAST(is_trip_still_profitable AS VARCHAR)
    } AS event_properties
FROM {{ ref('int_safety_incident') }}
