"""Append the 2024-05 JSON sample to the Bronze trips Delta table."""

import logging
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.bronze.export_json_source import ISO_TIMESTAMP, JSON_TRIPS
from src.bronze.ingest_postgres import _cast_bronze_columns
from src.bronze.ingest_trips import ensure_source_system
from src.common.config import BRONZE_TRIPS, EXTRA_MONTH_JSON
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

SOURCE_FILE = "json/trips_2024-05"

# JSON stores the two timestamps as ISO strings. Every other type matches Bronze.
_JSON_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", IntegerType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
    ]
)


def _month_loaded(spark, month: str) -> bool:
    table_path = BRONZE_TRIPS.as_posix()
    if not DeltaTable.isDeltaTable(spark, table_path):
        return False
    return (
        spark.read.format("delta")
        .load(table_path)
        .where(F.col("source_month") == month)
        .limit(1)
        .count()
        > 0
    )


def _parse_timestamps(frame):
    """Turn the exported ISO strings into timestamp_ntz before the Bronze cast."""
    for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        frame = frame.withColumn(
            name,
            F.to_timestamp(F.col(name), ISO_TIMESTAMP).cast("timestamp_ntz"),
        )
    return frame


def ingest_json(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        ensure_source_system(spark)
        if _month_loaded(spark, EXTRA_MONTH_JSON):
            logger.info("skipping %s; source_month already in bronze", EXTRA_MONTH_JSON)
            return
        if not JSON_TRIPS.exists():
            raise FileNotFoundError(f"missing json export: {JSON_TRIPS}")
        logger.info("ingesting %s from %s", EXTRA_MONTH_JSON, JSON_TRIPS)
        frame = spark.read.schema(_JSON_SCHEMA).json(JSON_TRIPS.as_posix())
        frame = _cast_bronze_columns(spark, _parse_timestamps(frame))
        frame = (
            frame.withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_file", F.lit(SOURCE_FILE))
            .withColumn("source_month", F.lit(EXTRA_MONTH_JSON))
            .withColumn("batch_id", F.lit(str(uuid.uuid4())))
            .withColumn("source_system", F.lit("json"))
        )
        (
            frame.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("source_month")
            .save(BRONZE_TRIPS.as_posix())
        )
        logger.info("appended %s from json", EXTRA_MONTH_JSON)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ingest_json()


if __name__ == "__main__":
    main()
