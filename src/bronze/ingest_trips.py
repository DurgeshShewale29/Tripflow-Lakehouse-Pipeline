"""Append each raw yellow-trip month to the Bronze Delta table."""

import logging
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import BRONZE_TRIPS, MONTHS, RAW_DIR
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

# These columns change type across TLC files. Cast them so every month appends cleanly.
_NUMERIC_COLUMNS = {
    "passenger_count": "int",
    "RatecodeID": "int",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
}


def _align_schema(frame):
    """Normalize drifting types. Rows and values are otherwise left untouched."""
    if "airport_fee" in frame.columns and "Airport_fee" in frame.columns:
        frame = frame.withColumn(
            "Airport_fee",
            F.coalesce(F.col("Airport_fee"), F.col("airport_fee")),
        ).drop("airport_fee")
    elif "airport_fee" in frame.columns:
        frame = frame.withColumnRenamed("airport_fee", "Airport_fee")
    elif "Airport_fee" not in frame.columns:
        frame = frame.withColumn("Airport_fee", F.lit(None).cast("double"))

    frame = frame.withColumn("Airport_fee", F.col("Airport_fee").cast("double"))
    for name, dtype in _NUMERIC_COLUMNS.items():
        frame = frame.withColumn(name, F.col(name).cast(dtype))
    return frame


def _loaded_months(spark) -> set[str]:
    table_path = BRONZE_TRIPS.as_posix()
    if not DeltaTable.isDeltaTable(spark, table_path):
        return set()
    rows = (
        spark.read.format("delta")
        .load(table_path)
        .select("source_month")
        .distinct()
        .collect()
    )
    return {row.source_month for row in rows}


def ensure_source_system(spark) -> None:
    """Add source_system once and mark rows loaded before this column existed."""
    table_path = BRONZE_TRIPS.as_posix()
    if not DeltaTable.isDeltaTable(spark, table_path):
        return
    current = spark.read.format("delta").load(table_path)
    if "source_system" not in current.columns:
        spark.sql(f"ALTER TABLE delta.`{table_path}` ADD COLUMNS (source_system STRING)")
        current = spark.read.format("delta").load(table_path)
    has_null = current.where(F.col("source_system").isNull()).limit(1).count() > 0
    if not has_null:
        return
    logger.info("setting source_system=parquet on existing bronze rows")
    DeltaTable.forPath(spark, table_path).update(
        condition="source_system IS NULL",
        set={"source_system": F.lit("parquet")},
    )


def ingest_trips(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        ensure_source_system(spark)
        loaded = _loaded_months(spark)
        batch_id = str(uuid.uuid4())
        table_path = BRONZE_TRIPS.as_posix()
        for month in MONTHS:
            if month in loaded:
                logger.info("skipping %s; source_month already in bronze", month)
                continue
            source = RAW_DIR / f"yellow_tripdata_{month}.parquet"
            if not source.is_file():
                raise FileNotFoundError(f"missing raw file: {source}")
            logger.info("ingesting %s", source.name)
            frame = _align_schema(spark.read.parquet(source.as_posix()))
            frame = (
                frame                .withColumn("ingest_ts", F.current_timestamp())
                .withColumn("source_file", F.lit(source.name))
                .withColumn("source_month", F.lit(month))
                .withColumn("batch_id", F.lit(batch_id))
                .withColumn("source_system", F.lit("parquet"))
            )
            (
                frame.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("source_month")
                .save(table_path)
            )
            logger.info("appended %s", month)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ingest_trips()


if __name__ == "__main__":
    main()
