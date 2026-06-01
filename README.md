# NYC Taxi Data Pipeline

> End-to-end batch data engineering pipeline for the NYC Yellow Taxi trip dataset. Ingests ~100M trips/year, transforms with PySpark, loads to Postgres, models with dbt, and serves analytics-ready tables.

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694A?logo=dbt)](https://www.getdbt.com/)
[![Postgres](https://img.shields.io/badge/Postgres-16-336791?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## What it does

Pulls the [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) from the city's public S3 bucket, cleans them with PySpark, lands them in a Postgres warehouse, builds analytics models with dbt, and exposes ready-to-query star-schema tables for BI tools.

| Metric | Value |
|---|---|
| Source format | Monthly Parquet on S3 (TLC public bucket) |
| Volume | ~100M trips/year (~10M trips/month) |
| Latency | Daily batch (1 DAG run per day) |
| Warehouse | Postgres 16 (swap for Redshift/BigQuery in prod) |
| Models | 4 staging + 3 marts (dim_zones, dim_dates, fct_daily_trips) |

## Architecture

```mermaid
flowchart LR
  A["NYC TLC<br/>S3 bucket<br/>(Parquet)"] -->|Airflow ingest| B[Raw zone<br/>./data/raw]
  B -->|PySpark transforms| C[Curated zone<br/>./data/curated]
  C -->|COPY| D[(Postgres<br/>raw schema)]
  D -->|dbt run| E[(Postgres<br/>marts schema)]
  E -->|BI / SQL| F["Tableau / Metabase /<br/>ad-hoc SQL"]
```

Airflow orchestrates the daily DAG:

1. **ingest_trips** — download last month's Yellow Taxi Parquet from TLC's S3 bucket.
2. **validate_schema** — assert column types and row counts; fail loud if upstream changed.
3. **spark_transform** — clean nulls, drop rides with negative fare, derive trip_minutes and price_per_mile, write partitioned Parquet.
4. **load_raw** — COPY curated Parquet into Postgres `raw.trips`.
5. **dbt_run** — build staging + marts (`fct_daily_trips`, `dim_zones`).
6. **dbt_test** — run dbt schema tests + custom data quality tests.
7. **publish_metrics** — push freshness + row-count metrics to the observability sink.

## Tech Stack

- **Orchestration:** Apache Airflow 2.9
- **Compute:** PySpark 3.5
- **Warehouse:** Postgres 16 (target = Redshift/BigQuery)
- **Transforms:** dbt 1.7 (staging + marts layers)
- **Storage:** local data lake (Parquet); swap for S3/GCS in prod
- **Infra:** Docker + docker-compose for local dev

## Quickstart (local)

```bash
git clone https://github.com/Rushikesh-S-Ware/nyc-taxi-data-pipeline
cd nyc-taxi-data-pipeline

# Boot Postgres, Airflow, and the Spark worker
docker-compose up -d

# Open Airflow UI
open http://localhost:8080   # default creds: admin / admin

# Trigger the daily DAG manually for the first run
docker-compose exec airflow airflow dags trigger nyc_taxi_daily
```

Once the DAG finishes, the analytics tables live in Postgres at `postgres://taxi:taxi@localhost:5432/taxi`, under the `marts` schema.

## Sample Queries

```sql
-- Top 10 busiest pickup zones, last 30 days
SELECT z.zone_name, COUNT(*) AS trips
FROM marts.fct_daily_trips f
JOIN marts.dim_zones z ON z.zone_id = f.pickup_zone_id
WHERE f.trip_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY z.zone_name
ORDER BY trips DESC
LIMIT 10;

-- Median fare per mile by hour of day
SELECT EXTRACT(HOUR FROM pickup_ts) AS hour_of_day,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY fare_per_mile) AS median_fare_per_mile
FROM marts.fct_daily_trips
GROUP BY 1
ORDER BY 1;
```

## Repository Layout

```
nyc-taxi-data-pipeline/
├── airflow/
│   └── dags/nyc_taxi_dag.py        # The daily ingest -> transform -> dbt DAG
├── spark/
│   ├── transforms.py               # PySpark cleaning + derivations
│   └── schemas.py                  # Expected schemas + validators
├── dbt/
│   ├── dbt_project.yml
│   ├── models/staging/stg_trips.sql
│   ├── models/marts/dim_zones.sql
│   └── models/marts/fct_daily_trips.sql
├── sql/
│   └── init.sql                    # Postgres schema bootstrap
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── LICENSE
└── README.md
```

## Design Decisions

- **Why a local data lake?** Keeps the project runnable on a laptop. In production, swap `./data/` for `s3://` (already abstracted in `spark/transforms.py`).
- **Why dbt on top of PySpark?** PySpark handles row-level cleaning; dbt owns business logic and tests. Clean separation of "data quality" vs. "business definitions".
- **Why Postgres and not Snowflake?** Same SQL surface, free to run, easy to swap. The dbt project targets a generic warehouse, not a vendor lock-in.
- **Why daily and not streaming?** Source data is published monthly. Streaming would be expensive theater; daily batch is the right shape.

## Roadmap

- [ ] Add Great Expectations data-quality suite
- [ ] Materialize `fct_daily_trips` as a Spark Delta table for time-travel
- [ ] Swap Postgres target for Snowflake or BigQuery (one-line dbt profile change)
- [ ] Add a Tableau workbook + screenshot in `/docs`

## License

MIT
# nyc-taxi-data-pipeline
End-to-end data engineering pipeline for NYC Yellow Taxi trips. Airflow + PySpark + dbt + Postgres + Docker. ~100M rows/year.
