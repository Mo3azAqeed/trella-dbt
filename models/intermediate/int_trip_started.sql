{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='intermediate'
    )
}}
SELECT
    t.trip_id,
    pickup.actual_datetime AS created_at,
    t.load_id,
    l.route_id,
    t.driver_id,
    t.truck_id,
    t.trailer_id,
    pickup.facility_id AS origin_facility_id,
    pickup.location_city AS origin_city,
    pickup.location_state AS origin_state,

    l.revenue AS potential_revenue,

    delivery.scheduled_datetime AS estimated_arrival_at,
    delivery.actual_datetime AS actual_arrival_at,

    pickup.detention_minutes AS pickup_detention_minutes,

    pickup.on_time_flag AS is_pickup_on_time

FROM {{ ref('stg_trips') }} AS t
INNER JOIN {{ ref('stg_delivery_events') }} AS pickup
    ON t.trip_id = pickup.trip_id
    AND pickup.event_type = 'Pickup'
LEFT JOIN {{ ref('stg_delivery_events') }} AS delivery
    ON t.trip_id = delivery.trip_id
    AND delivery.event_type = 'Delivery'
LEFT JOIN {{ ref('stg_loads') }} AS l
    ON t.load_id = l.load_id