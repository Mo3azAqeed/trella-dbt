{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT
    l.load_id,
    de.scheduled_datetime as created_at,
    de.trip_id,
    l.route_id,
    l.customer_id,
    de.facility_id,
    l.load_type as equipment_required,
    l.booking_type as booking_channel,
    l.revenue as financial_base,
    (l.fuel_surcharge + l.accessorial_charges) as financial_extras,
    (l.revenue + l.fuel_surcharge + l.accessorial_charges) as financial_total,
    r.typical_distance_miles as route_distance,    
    -- KEY METRIC: Actual Revenue Per Mile
        -- Logic: Base Revenue / Miles. 
    (l.revenue / NULLIF(r.typical_distance_miles, 0)) as actual_rpm,
    -- Benchmark Variance (Did we beat the standard rate?)
    r.base_rate_per_mile as benchmark_rpm,
    ((l.revenue / NULLIF(r.typical_distance_miles, 0)) - r.base_rate_per_mile) as rpm_variance
FROM {{ ref('stg_loads') }} as l
LEFT JOIN {{ ref('stg_routes') }} as r
    ON l.route_id = r.route_id
LEFT JOIN {{ ref('stg_delivery_events') }} as de
    ON l.load_id = de.load_id
WHERE de.event_type = 'Pickup'
