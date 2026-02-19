{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='intermediate'
    )
}}

SELECT
    de.trip_id,
    de.actual_datetime AS created_at,
    de.load_id,
    de.facility_id,
    de.location_city AS delivery_city,
    de.location_state AS delivery_state,
    de.on_time_flag,
    de.detention_minutes,

    -- Trip context
    t.driver_id,
    t.truck_id,
    t.trailer_id,
    t.actual_distance_miles,
    t.actual_duration_hours,

    -- Revenue
    l.customer_id,
    l.route_id,
    (l.revenue + l.fuel_surcharge + l.accessorial_charges) AS total_revenue,

    -- Cost components
    COALESCE(fp.total_fuel_cost, 0) AS fuel_cost,
    ROUND(COALESCE(t.actual_duration_hours, 0) * 25.0, 2) AS driver_cost,
    ROUND(COALESCE(de.detention_minutes, 0) * (50.0 / 60), 2) AS detention_cost,

    -- Total cost = fuel + driver + detention
    ROUND(
        COALESCE(fp.total_fuel_cost, 0)
        + COALESCE(t.actual_duration_hours, 0) * 25.0
        + COALESCE(de.detention_minutes, 0) * (50.0 / 60),
        2
    ) AS total_cost,

    -- Total profit = total revenue - total cost
    ROUND(
        (l.revenue + l.fuel_surcharge + l.accessorial_charges)
        - (
            COALESCE(fp.total_fuel_cost, 0)
            + COALESCE(t.actual_duration_hours, 0) * 25.0
            + COALESCE(de.detention_minutes, 0) * (50.0 / 60)
        ),
        2
    ) AS total_profit

FROM {{ ref('stg_delivery_events') }} AS de
INNER JOIN {{ ref('stg_trips') }} AS t
    ON de.trip_id = t.trip_id
LEFT JOIN {{ ref('stg_loads') }} AS l
    ON de.load_id = l.load_id
LEFT JOIN (
    SELECT trip_id, SUM(total_cost) AS total_fuel_cost
    FROM {{ ref('stg_fuel_purchases') }}
    GROUP BY trip_id
) AS fp
    ON de.trip_id = fp.trip_id
WHERE de.event_type = 'Delivery'
  AND de.actual_datetime IS NOT NULL
