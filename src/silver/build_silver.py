"""Build Silver trips, quarantine rejected rows, and reconcile each month."""

import logging

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import (
    BRONZE_TRIPS,
    BRONZE_ZONES,
    MONTHS,
    QUARANTINE_TRIPS,
    SILVER_RECON,
    SILVER_TRIPS,
)
from src.common.spark_session import get_spark
from src.silver.rules import with_reject_reason

logger = logging.getLogger(__name__)

_DEDUP_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]

_RECON_SCHEMA = StructType(
    [
        StructField("source_month", StringType()),
        StructField("bronze_rows", LongType()),
        StructField("silver_rows", LongType()),
        StructField("quarantined_rows", LongType()),
        StructField("duplicates_removed", LongType()),
    ]
)


def drop_duplicate_trips(frame):
    """Drop duplicate trips on the bronze business key."""
    return frame.dropDuplicates(_DEDUP_COLUMNS)


def reconciliation_holds(
    bronze_rows: int,
    silver_rows: int,
    quarantined_rows: int,
    duplicates_removed: int,
) -> bool:
    """True when every bronze row is silver, quarantined, or a removed duplicate."""
    return bronze_rows == silver_rows + quarantined_rows + duplicates_removed


def _save_month(spark, frame, path: str, month: str, partition: bool) -> None:
    """Create the table on first write; later runs replace only this source_month."""
    writer = frame.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, path):
        writer = writer.option("replaceWhere", f"source_month = '{month}'")
    if partition:
        writer = writer.partitionBy("source_month")
    writer.save(path)


def _to_silver(trips, zones):
    """Rename valid Bronze rows and attach zone names with a broadcast join."""
    zone_dim = (
        zones.select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Zone").alias("zone_name"),
            F.col("Borough").alias("borough"),
        )
        .where(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
    )
    pickup_zones = zone_dim.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("zone_name").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zone_dim.select(
        F.col("location_id").alias("do_location_id"),
        F.col("zone_name").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    renamed = trips.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        "passenger_count",
        "trip_distance",
        F.col("RatecodeID").alias("rate_code_id"),
        "store_and_fwd_flag",
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        F.col("Airport_fee").alias("airport_fee"),
        "source_month",
        "batch_id",
    )
    enriched = renamed.join(F.broadcast(pickup_zones), "pu_location_id", "left").join(
        F.broadcast(dropoff_zones), "do_location_id", "left"
    )
    return enriched.select(
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        (F.expr("timestampdiff(SECOND, pickup_ts, dropoff_ts)") / F.lit(60.0)).alias("trip_duration_min"),
        F.to_date("pickup_ts").alias("pickup_date"),
        F.hour("pickup_ts").alias("pickup_hour"),
        "pickup_zone",
        "pickup_borough",
        "dropoff_zone",
        "dropoff_borough",
        "source_month",
        "batch_id",
        F.current_timestamp().alias("silver_ts"),
    )


def _build_month(spark, zones, month: str) -> None:
    logger.info("building silver for %s", month)
    bronze = spark.read.format("delta").load(BRONZE_TRIPS.as_posix()).where(F.col("source_month") == month)
    bronze_rows = bronze.count()
    classified = with_reject_reason(bronze, zones).cache()
    deduped = None
    try:
        valid = classified.filter(F.col("reject_reason").isNull())
        quarantine = classified.filter(F.col("reject_reason").isNotNull()).withColumn(
            "quarantined_ts", F.current_timestamp()
        )
        valid_rows = valid.count()
        quarantined_rows = quarantine.count()
        deduped = drop_duplicate_trips(valid.drop("reject_reason")).cache()
        duplicates_removed = valid_rows - deduped.count()
        silver = _to_silver(deduped, zones)
        silver_rows = silver.count()

        # bronze_rows must equal the rows kept, rejected, and removed as duplicates.
        if not reconciliation_holds(bronze_rows, silver_rows, quarantined_rows, duplicates_removed):
            logger.error(
                "reconciliation failed for %s: bronze_rows=%s silver_rows=%s quarantined_rows=%s duplicates_removed=%s",
                month,
                bronze_rows,
                silver_rows,
                quarantined_rows,
                duplicates_removed,
            )
            raise RuntimeError(f"reconciliation failed for {month}")

        _save_month(spark, quarantine, QUARANTINE_TRIPS.as_posix(), month, partition=True)
        _save_month(spark, silver, SILVER_TRIPS.as_posix(), month, partition=True)
        recon = spark.createDataFrame(
            [(month, bronze_rows, silver_rows, quarantined_rows, duplicates_removed)],
            schema=_RECON_SCHEMA,
        )
        _save_month(spark, recon, SILVER_RECON.as_posix(), month, partition=False)
        logger.info(
            "source_month=%s bronze_rows=%s silver_rows=%s quarantined_rows=%s duplicates_removed=%s",
            month,
            bronze_rows,
            silver_rows,
            quarantined_rows,
            duplicates_removed,
        )
    finally:
        classified.unpersist()
        if deduped is not None:
            deduped.unpersist()


def _log_summary(spark) -> None:
    recon_rows = (
        spark.read.format("delta").load(SILVER_RECON.as_posix()).orderBy("source_month").collect()
    )
    logger.info("summary source_month bronze_rows silver_rows quarantined_rows duplicates_removed")
    for row in recon_rows:
        logger.info(
            "%s %s %s %s %s",
            row.source_month,
            row.bronze_rows,
            row.silver_rows,
            row.quarantined_rows,
            row.duplicates_removed,
        )

    reasons = (
        spark.read.format("delta")
        .load(QUARANTINE_TRIPS.as_posix())
        .groupBy("reject_reason")
        .count()
        .orderBy(F.desc("count"), "reject_reason")
        .collect()
    )
    logger.info("quarantine by reject_reason")
    if not reasons:
        logger.info("no quarantined rows")
        return
    for row in reasons:
        logger.info("%s %s", row.reject_reason, row["count"])


def build_silver(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        zones = spark.read.format("delta").load(BRONZE_ZONES.as_posix())
        for month in MONTHS:
            _build_month(spark, zones, month)
        _log_summary(spark)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    build_silver()


if __name__ == "__main__":
    main()
