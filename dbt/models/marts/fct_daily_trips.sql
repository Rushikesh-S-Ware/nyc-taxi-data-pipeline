-- Mart: daily trip facts aggregated by pickup_date and pickup_zone.
-- Incrementally materialized; new partitions append daily.
{{ config(
    materialized='incremental',
    unique_key=['trip_date', 'pickup_zone_id'],
    on_schema_change='fail'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
    {% if is_incremental() %}
    WHERE pickup_date > (SELECT COALESCE(MAX(trip_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

aggregated AS (
    SELECT
        pickup_date                            AS trip_date,
        pickup_zone_id,
        COUNT(*)                               AS trip_count,
        SUM(passenger_count)                   AS passenger_total,
        ROUND(AVG(trip_distance)::numeric, 3)  AS avg_distance,
        ROUND(AVG(trip_minutes)::numeric, 2)   AS avg_minutes,
        ROUND(AVG(fare_amount)::numeric, 2)    AS avg_fare,
        ROUND(AVG(price_per_mile)::numeric, 3) AS avg_fare_per_mile,
        ROUND(SUM(tip_amount)::numeric, 2)     AS total_tips,
        ROUND(SUM(total_amount)::numeric, 2)   AS total_revenue
    FROM trips
    GROUP BY pickup_date, pickup_zone_id
)

SELECT * FROM aggregated
