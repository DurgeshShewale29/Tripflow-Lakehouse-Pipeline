"""Append the PostgreSQL April sample to the Bronze trips Delta table."""

import logging
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.bronze.ingest_trips import _align_schema, ensure_source_system
from src.bronze.load_postgres_source import TABLE, _jdbc_reader
from src.common.config import BRONZE_TRIPS, EXTRA_MONTH_PG
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

SOURCE_FILE = "postgres:public.yellow_trips_source"

# Added after the JDBC read, so they are not cast from the Postgres table.
_METADATA = ("ingest_ts", "source_file", "source_month", "batch_id", "source_system")


def _cast_bronze_columns(spark, frame):
    """Match the Bronze trips schema. JDBC returns the pickup timestamps as timestamp."""
    frame = _align_schema(frame)
    frame = frame.withColumn(
        "tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp_ntz")
    ).withColumn(
        "tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp_ntz")
    )
    table_path = BRONZE_TRIPS.as_posix()
    if not DeltaTable.isDeltaTable(spark, table_path):
        return frame
    selected = []
    for field in spark.read.format("delta").load(table_path).schema.fields:
        if field.name in _METADATA:
            continue
        if field.name not in frame.columns:
            selected.append(F.lit(None).cast(field.dataType).alias(field.name))
        else:
            selected.append(F.col(field.name).cast(field.dataType).alias(field.name))
    return frame.select(selected)


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


def ingest_postgres(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        ensure_source_system(spark)
        if _month_loaded(spark, EXTRA_MONTH_PG):
            logger.info("skipping %s; source_month already in bronze", EXTRA_MONTH_PG)
            return
        logger.info("ingesting %s from %s", EXTRA_MONTH_PG, TABLE)
        frame = _cast_bronze_columns(spark, _jdbc_reader(spark, TABLE).load())
        frame = (
            frame.withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_file", F.lit(SOURCE_FILE))
            .withColumn("source_month", F.lit(EXTRA_MONTH_PG))
            .withColumn("batch_id", F.lit(str(uuid.uuid4())))
            .withColumn("source_system", F.lit("postgres"))
        )
        (
            frame.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("source_month")
            .save(BRONZE_TRIPS.as_posix())
        )
        logger.info("appended %s from postgres", EXTRA_MONTH_PG)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ingest_postgres()


if __name__ == "__main__":
    main()
