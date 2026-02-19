{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='intermediate'
    )
}}

SELECT
    si.incident_id,
    si.incident_date AS created_at,
    si.trip_id,
    si.truck_id,
    si.driver_id,
    si.incident_type,
    si.location_city AS incident_city,
    si.location_state AS incident_state,
    si.at_fault_flag,
    si.injury_flag,
    si.preventable_flag,
    si.description,

    -- Damage costs
    si.vehicle_damage_cost,
    si.cargo_damage_cost,
    (si.vehicle_damage_cost + si.cargo_damage_cost) AS total_damage_cost,
    si.claim_amount,

    -- Trip context
    t.load_id,
    t.actual_distance_miles,
    t.actual_duration_hours,

    l.customer_id,
    l.route_id,
    (l.revenue + l.fuel_surcharge + l.accessorial_charges) AS trip_total_revenue,

    ROUND(
        (l.revenue + l.fuel_surcharge + l.accessorial_charges)
        - (si.vehicle_damage_cost + si.cargo_damage_cost),
        2
    ) AS revenue_after_damage,

    CASE
        WHEN (l.revenue + l.fuel_surcharge + l.accessorial_charges)
             - (si.vehicle_damage_cost + si.cargo_damage_cost) > 0
        THEN TRUE
        ELSE FALSE
    END AS is_trip_still_profitable

FROM {{ ref('stg_safety_incidents') }} AS si
INNER JOIN {{ ref('stg_trips') }} AS t
    ON si.trip_id = t.trip_id
LEFT JOIN {{ ref('stg_loads') }} AS l
    ON t.load_id = l.load_id
