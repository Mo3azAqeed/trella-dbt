{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='intermediate'
    )
}}

SELECT
    de.load_id,
    de.scheduled_datetime AS created_at,
    de.trip_id,
    de.facility_id,
    de.location_city AS delivery_city,
    de.location_state AS delivery_state,

    -- On-time delivery flag
    de.on_time_flag,

    -- Detention at delivery (minutes waiting to unload)
    de.detention_minutes AS detention_mins,

    -- Detention Cost: estimated cost of the wait
    -- Driver Pay component (~$25/hr) + Opportunity Cost (~$25/hr) = ~$50/hr
    ROUND(de.detention_minutes * (50.0 / 60), 2) AS detention_cost,

    -- Scheduled vs actual delivery for variance analysis
    de.scheduled_datetime AS scheduled_delivery_at,
    de.actual_datetime AS actual_delivery_at,

    -- Delivery variance in hours (positive = late, negative = early)
    ROUND(
        EXTRACT(EPOCH FROM (de.actual_datetime - de.scheduled_datetime)) / 3600.0,
        2
    ) AS delivery_variance_hours,

    -- Trip context
    t.driver_id,
    t.truck_id,
    t.trailer_id,
    t.actual_distance_miles,
    t.actual_duration_hours,

    -- Load context
    l.customer_id,
    l.route_id,
    l.revenue AS potential_revenue,
    (l.revenue + l.fuel_surcharge + l.accessorial_charges) AS financial_total

FROM {{ ref('stg_delivery_events') }} AS de
INNER JOIN {{ ref('stg_trips') }} AS t
    ON de.trip_id = t.trip_id
LEFT JOIN {{ ref('stg_loads') }} AS l
    ON de.load_id = l.load_id
WHERE de.event_type = 'Delivery'
