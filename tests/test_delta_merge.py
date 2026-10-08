"""Delta merge updates, Silver dedup, and the bronze reconciliation identity."""

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from src.gold.build_gold import _merge
from src.silver.build_silver import drop_duplicate_trips, reconciliation_holds
from src.silver.rules import with_reject_reason

DEDUP_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("source_month", StringType()),
        StructField("trip_distance", DoubleType()),
        StructField("passenger_count", IntegerType()),
    ]
)


def test_merge_second_run_updates(spark, tmp_path):
    path = (tmp_path / "daily").as_posix()
    condition = "t.pickup_date <=> s.pickup_date"
    first = spark.createDataFrame(
        [("2024-03-01", 10.0), ("2024-03-02", 20.0)],
        ["pickup_date", "total_revenue"],
    )
    _merge(spark, first, path, condition, partition=False)
    assert spark.read.format("delta").load(path).count() == 2

    second = spark.createDataFrame(
        [("2024-03-01", 15.0), ("2024-03-02", 25.0)],
        ["pickup_date", "total_revenue"],
    )
    _merge(spark, second, path, condition, partition=False)

    rows = spark.read.format("delta").load(path).orderBy("pickup_date").collect()
    assert len(rows) == 2
    assert [row.total_revenue for row in rows] == [15.0, 25.0]

    latest = DeltaTable.forPath(spark, path).history().orderBy(F.desc("version")).limit(1).collect()[0]
    metrics = latest.operationMetrics
    assert int(metrics["numTargetRowsInserted"]) == 0
    assert int(metrics["numTargetRowsUpdated"]) == 2


def test_dedup_and_reconciliation(spark):
    pickup = datetime(2024, 3, 1, 8, 0)
    dropoff = datetime(2024, 3, 1, 8, 20)
    duplicate = (1, pickup, dropoff, 1, 132, 10.0, 15.0, "2024-03", 2.5, 1)
    other = (1, datetime(2024, 3, 1, 9, 0), datetime(2024, 3, 1, 9, 15), 1, 132, 12.0, 18.0, "2024-03", 3.0, 1)
    rejected = (1, pickup, dropoff, 1, 132, -1.0, 15.0, "2024-03", 2.5, 1)
    trips = spark.createDataFrame([duplicate, duplicate, other, rejected], DEDUP_SCHEMA)

    classified = with_reject_reason(trips, [1, 132])
    valid = classified.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    quarantined_rows = classified.filter(F.col("reject_reason").isNotNull()).count()
    deduped = drop_duplicate_trips(valid)
    duplicates_removed = valid.count() - deduped.count()
    silver_rows = deduped.count()

    assert deduped.count() == 2
    assert quarantined_rows == 1
    assert duplicates_removed == 1
    assert reconciliation_holds(trips.count(), silver_rows, quarantined_rows, duplicates_removed)
    assert not reconciliation_holds(trips.count(), silver_rows, quarantined_rows, 0)
