-- Staging model: surface the raw trips with consistent naming and types.
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'trips') }}
),

renamed AS (
    SELECT
        pickup_ts::timestamp           AS pickup_ts,
        dropoff_ts::timestamp          AS dropoff_ts,
        pickup_date::date              AS pickup_date,
        pickup_zone_id::int            AS pickup_zone_id,
        dropoff_zone_id::int           AS dropoff_zone_id,
        passenger_count::int           AS passenger_count,
        trip_distance::numeric         AS trip_distance,
        trip_minutes::int              AS trip_minutes,
        fare_amount::numeric           AS fare_amount,
        tip_amount::numeric            AS tip_amount,
        total_amount::numeric          AS total_amount,
        price_per_mile::numeric        AS price_per_mile,
        payment_type::int              AS payment_type
    FROM source
    WHERE pickup_ts IS NOT NULL
      AND fare_amount >= 0
)

SELECT * FROM renamed
