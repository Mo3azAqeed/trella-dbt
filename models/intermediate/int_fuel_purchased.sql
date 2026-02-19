{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='intermediate'
    )
}}

SELECT
    fp.fuel_purchase_id,
    fp.purchase_date AS created_at,
    fp.trip_id,
    fp.truck_id,
    fp.driver_id,
    fp.location_city AS fuel_city,
    fp.location_state AS fuel_state,
    fp.gallons,
    fp.price_per_gallon,
    fp.total_cost AS fuel_cost,
    fp.fuel_card_number,

    -- Trip context
    t.load_id,
    t.actual_distance_miles,
    t.actual_duration_hours,

    -- Load context
    l.customer_id,
    l.route_id

FROM {{ ref('stg_fuel_purchases') }} AS fp
INNER JOIN {{ ref('stg_trips') }} AS t
    ON fp.trip_id = t.trip_id
LEFT JOIN {{ ref('stg_loads') }} AS l
    ON t.load_id = l.load_id
