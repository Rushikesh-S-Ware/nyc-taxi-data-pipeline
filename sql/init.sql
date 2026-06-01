-- Bootstrap Postgres schemas for the NYC Taxi pipeline.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw landing zone for Spark-cleaned Yellow Taxi trips.
CREATE TABLE IF NOT EXISTS raw.trips (
    pickup_ts         TIMESTAMP    NOT NULL,
    dropoff_ts        TIMESTAMP    NOT NULL,
    pickup_date       DATE         NOT NULL,
    pickup_zone_id    INT          NOT NULL,
    dropoff_zone_id   INT          NOT NULL,
    passenger_count   INT,
    trip_distance     NUMERIC(8,3),
    trip_minutes      INT,
    fare_amount       NUMERIC(10,2),
    tip_amount        NUMERIC(10,2),
    total_amount      NUMERIC(10,2),
    price_per_mile    NUMERIC(10,3),
    payment_type      INT
);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_date ON raw.trips (pickup_date);
CREATE INDEX IF NOT EXISTS idx_trips_pickup_zone ON raw.trips (pickup_zone_id);

-- Zone dimension seeded from TLC's published zone lookup CSV.
CREATE TABLE IF NOT EXISTS raw.zones (
    zone_id    INT PRIMARY KEY,
    borough    TEXT,
    zone_name  TEXT,
    service    TEXT
);
