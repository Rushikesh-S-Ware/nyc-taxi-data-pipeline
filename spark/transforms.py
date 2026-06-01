"""PySpark transforms: clean NYC TLC Yellow Taxi trips for the warehouse."""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def _spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nyc-taxi-clean")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )


def clean_trips(input_path: str, output_path: str) -> int:
    """Read raw TLC parquet, clean it, write curated parquet. Returns curated row count."""
    spark = _spark()
    df: DataFrame = spark.read.parquet(input_path)

    # Drop rows with missing pickup/dropoff timestamps or zone IDs.
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"])

    # Filter obvious garbage: negative fares, zero-distance, or > 6-hour trips.
    df = df.filter((F.col("fare_amount") >= 0) & (F.col("trip_distance") > 0))
    trip_minutes = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    df = df.withColumn("trip_minutes", trip_minutes.cast(IntegerType()))
    df = df.filter((F.col("trip_minutes") > 0) & (F.col("trip_minutes") < 360))

    # Derive analytics-friendly columns.
    df = (
        df
        .withColumn("price_per_mile", F.col("fare_amount") / F.col("trip_distance"))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumnRenamed("tpep_pickup_datetime", "pickup_ts")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_ts")
        .withColumnRenamed("PULocationID", "pickup_zone_id")
        .withColumnRenamed("DOLocationID", "dropoff_zone_id")
    )

    # Partition by pickup_date for incremental dbt + cheap pruning.
    df.write.mode("overwrite").partitionBy("pickup_date").parquet(output_path)
    return df.count()


if __name__ == "__main__":  # pragma: no cover
    import sys
    clean_trips(sys.argv[1], sys.argv[2])
