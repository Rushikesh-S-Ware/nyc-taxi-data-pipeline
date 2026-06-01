"""Daily DAG: ingest NYC TLC Yellow Taxi trips, transform with Spark, load to Postgres, build dbt models."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from spark.transforms import clean_trips
from spark.schemas import validate_raw_schema

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/opt/airflow/data"))
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PG_CONN_ID = "taxi_postgres"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _month_key(execution_date) -> str:
    # TLC publishes one Parquet per month with a one-month lag.
    prior = execution_date - timedelta(days=execution_date.day + 1)
    return prior.strftime("%Y-%m")


def ingest_trips(**ctx):
    """Download the prior-month Yellow Taxi Parquet from TLC."""
    import urllib.request

    month = _month_key(ctx["execution_date"])
    url = f"{TLC_BASE}/yellow_tripdata_{month}.parquet"
    out = DATA_ROOT / "raw" / f"yellow_{month}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, out)
    ctx["ti"].xcom_push(key="raw_path", value=str(out))
    return str(out)


def validate_schema(**ctx):
    raw_path = ctx["ti"].xcom_pull(task_ids="ingest_trips", key="raw_path")
    validate_raw_schema(raw_path)


def spark_transform(**ctx):
    """Clean nulls, drop invalid rows, derive trip_minutes + price_per_mile."""
    raw_path = ctx["ti"].xcom_pull(task_ids="ingest_trips", key="raw_path")
    curated_path = DATA_ROOT / "curated" / Path(raw_path).name
    clean_trips(input_path=raw_path, output_path=str(curated_path))
    ctx["ti"].xcom_push(key="curated_path", value=str(curated_path))


def load_raw(**ctx):
    """COPY curated Parquet into Postgres raw.trips."""
    curated_path = ctx["ti"].xcom_pull(task_ids="spark_transform", key="curated_path")
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(
        "COPY raw.trips FROM PROGRAM 'parquet-tools cat ' || %s WITH (FORMAT csv, HEADER true);",
        parameters=[curated_path],
    )


def publish_metrics(**ctx):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    rows = hook.get_first("SELECT COUNT(*) FROM marts.fct_daily_trips WHERE trip_date >= CURRENT_DATE - 31;")
    print(f"[metrics] fct_daily_trips rows last 31d = {rows[0]}")


with DAG(
    dag_id="nyc_taxi_daily",
    description="Ingest, transform, load, and model NYC Yellow Taxi trips daily.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-eng", "taxi", "dbt"],
) as dag:

    ingest = PythonOperator(task_id="ingest_trips", python_callable=ingest_trips, provide_context=True)
    validate = PythonOperator(task_id="validate_schema", python_callable=validate_schema, provide_context=True)
    transform = PythonOperator(task_id="spark_transform", python_callable=spark_transform, provide_context=True)
    load = PythonOperator(task_id="load_raw", python_callable=load_raw, provide_context=True)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    metrics = PythonOperator(task_id="publish_metrics", python_callable=publish_metrics, provide_context=True)

    ingest >> validate >> transform >> load >> dbt_run >> dbt_test >> metrics
