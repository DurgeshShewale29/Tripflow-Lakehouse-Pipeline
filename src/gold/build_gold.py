"""Aggregate Silver into Gold tables with an incremental Delta merge."""

import argparse
import logging

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import (
    GOLD_DAILY_SUMMARY,
    GOLD_PAYMENT_BOROUGH,
    GOLD_REVENUE_ZONE_HOUR,
    SILVER_TRIPS,
)
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

_GOLD_TABLES = (
    ("gold_revenue_by_zone_hour", GOLD_REVENUE_ZONE_HOUR),
    ("gold_daily_summary", GOLD_DAILY_SUMMARY),
    ("gold_payment_borough", GOLD_PAYMENT_BOROUGH),
)


def _revenue(month_df, month: str):
    return month_df.groupBy("pickup_date", "pickup_hour", "pu_location_id").agg(
        F.max("pickup_zone").alias("pickup_zone"),
        F.max("pickup_borough").alias("pickup_borough"),
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.lit(month).alias("source_month"),
        F.current_timestamp().alias("gold_ts"),
    )


def _daily(month_df, month: str):
    tip_pct = F.when(F.sum("fare_amount") != 0, F.sum("tip_amount") / F.sum("fare_amount"))
    return month_df.groupBy("pickup_date").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(tip_pct, 4).alias("tip_pct"),
        F.lit(month).alias("source_month"),
        F.current_timestamp().alias("gold_ts"),
    )


def _payment(month_df):
    return month_df.groupBy("source_month", "pickup_borough", "payment_type").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    )


def _merge(spark, frame, path: str, condition: str, partition: bool) -> None:
    """Create an empty Delta table once, then insert or update matching keys."""
    if not DeltaTable.isDeltaTable(spark, path):
        writer = frame.limit(0).write.format("delta")
        if partition:
            writer = writer.partitionBy("source_month")
        writer.save(path)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(frame.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_month(spark, month_df, month: str) -> None:
    logger.info("merging gold for %s", month)
    _merge(
        spark,
        _revenue(month_df, month),
        GOLD_REVENUE_ZONE_HOUR.as_posix(),
        "t.pickup_date <=> s.pickup_date AND t.pickup_hour <=> s.pickup_hour AND t.pu_location_id <=> s.pu_location_id",
        partition=True,
    )
    _merge(
        spark,
        _daily(month_df, month),
        GOLD_DAILY_SUMMARY.as_posix(),
        "t.pickup_date <=> s.pickup_date",
        partition=False,
    )
    _merge(
        spark,
        _payment(month_df),
        GOLD_PAYMENT_BOROUGH.as_posix(),
        "t.source_month <=> s.source_month AND t.pickup_borough <=> s.pickup_borough AND t.payment_type <=> s.payment_type",
        partition=False,
    )


def _source_months(spark, months: list[str] | None) -> list[str]:
    if months:
        return months
    rows = (
        spark.read.format("delta")
        .load(SILVER_TRIPS.as_posix())
        .select("source_month")
        .distinct()
        .orderBy("source_month")
        .collect()
    )
    return [row.source_month for row in rows]


def _log_results(spark) -> None:
    for name, path in _GOLD_TABLES:
        row_count = spark.read.format("delta").load(path.as_posix()).count()
        logger.info("%s rows=%s", name, row_count)

    top_rows = (
        spark.read.format("delta")
        .load(GOLD_REVENUE_ZONE_HOUR.as_posix())
        .orderBy(F.desc("total_revenue"))
        .select(
            "pickup_date",
            "pickup_hour",
            "pu_location_id",
            "pickup_zone",
            "pickup_borough",
            "trips",
            "total_revenue",
        )
        .limit(10)
        .collect()
    )
    logger.info("top zone-hour by total_revenue")
    for row in top_rows:
        logger.info(
            "pickup_date=%s pickup_hour=%s pu_location_id=%s pickup_zone=%s pickup_borough=%s trips=%s total_revenue=%s",
            row.pickup_date,
            row.pickup_hour,
            row.pu_location_id,
            row.pickup_zone,
            row.pickup_borough,
            row.trips,
            row.total_revenue,
        )


def _check_daily_trips(spark, months: list[str]) -> None:
    silver_rows = (
        spark.read.format("delta")
        .load(SILVER_TRIPS.as_posix())
        .where(F.col("source_month").isin(months))
        .count()
    )
    gold_trips = (
        spark.read.format("delta")
        .load(GOLD_DAILY_SUMMARY.as_posix())
        .where(F.col("source_month").isin(months))
        .agg(F.coalesce(F.sum("trips"), F.lit(0)).alias("trips"))
        .collect()[0]["trips"]
    )
    if int(silver_rows) != int(gold_trips):
        logger.error(
            "gold daily trips mismatch: silver_rows=%s gold_trips=%s months=%s",
            silver_rows,
            gold_trips,
            ",".join(months),
        )
        raise RuntimeError("gold_daily_summary trips do not match the silver row count")
    logger.info("check ok silver_rows=%s gold_trips=%s", silver_rows, gold_trips)


def build_gold(spark=None, months: list[str] | None = None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        selected = _source_months(spark, months)
        if not selected:
            logger.info("no silver source months to process")
            return
        for month in selected:
            month_df = (
                spark.read.format("delta")
                .load(SILVER_TRIPS.as_posix())
                .where(F.col("source_month") == month)
                .cache()
            )
            try:
                _merge_month(spark, month_df, month)
            finally:
                month_df.unpersist()
        _log_results(spark)
        _check_daily_trips(spark, selected)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", default=None)
    args = parser.parse_args()
    build_gold(months=args.months)


if __name__ == "__main__":
    main()
