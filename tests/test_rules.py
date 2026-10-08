"""Reject-reason coverage for the Bronze quality rules."""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.silver.rules import with_reject_reason

# Parsed in the session timezone so the month does not shift across machines.
PICKUP = "2024-03-01 08:00:00"
DROPOFF = "2024-03-01 08:20:00"
ZONES = [1, 132]

TRIP_SCHEMA = StructType(
    [
        StructField("trip_id", StringType()),
        StructField("pickup_text", StringType()),
        StructField("dropoff_text", StringType()),
        StructField("source_month", StringType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("passenger_count", IntegerType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
    ]
)


def _trip(
    trip_id,
    pickup=PICKUP,
    dropoff=DROPOFF,
    month="2024-03",
    distance=2.5,
    fare=10.0,
    total=15.0,
    passengers=1,
    pu=1,
    do=132,
):
    return (trip_id, pickup, dropoff, month, distance, fare, total, passengers, pu, do)


def _reasons(spark, rows):
    frame = spark.createDataFrame(rows, TRIP_SCHEMA)
    frame = frame.withColumn("tpep_pickup_datetime", F.to_timestamp("pickup_text")).withColumn(
        "tpep_dropoff_datetime", F.to_timestamp("dropoff_text")
    )
    flagged = with_reject_reason(frame, ZONES)
    return {row.trip_id: row.reject_reason for row in flagged.select("trip_id", "reject_reason").collect()}


def test_each_reject_reason(spark):
    reasons = _reasons(
        spark,
        [
            _trip("valid"),
            _trip("null_timestamps", pickup=None),
            _trip("dropoff_before_pickup", pickup="2024-03-01 09:00:00", dropoff="2024-03-01 08:00:00"),
            _trip("outside_month", pickup="2024-02-15 08:00:00", dropoff="2024-02-15 08:20:00"),
            _trip("over_24h", pickup="2024-03-01 00:00:00", dropoff="2024-03-02 00:01:00"),
            _trip("invalid_distance", distance=0.0),
            _trip("negative_fare", fare=-1.0),
            _trip("invalid_passengers", passengers=10),
            _trip("unknown_pickup", pu=999),
            _trip("unknown_dropoff", do=999),
        ],
    )
    assert reasons["valid"] is None
    assert reasons["null_timestamps"] == "null_pickup_or_dropoff"
    assert reasons["dropoff_before_pickup"] == "dropoff_not_after_pickup"
    assert reasons["outside_month"] == "pickup_outside_source_month"
    assert reasons["over_24h"] == "duration_over_24h"
    assert reasons["invalid_distance"] == "invalid_distance"
    assert reasons["negative_fare"] == "negative_fare_or_total"
    assert reasons["invalid_passengers"] == "invalid_passenger_count"
    assert reasons["unknown_pickup"] == "unknown_pickup_zone"
    assert reasons["unknown_dropoff"] == "unknown_dropoff_zone"


def test_first_failing_rule_wins(spark):
    # Null timestamps outrank later failures such as distance and fare.
    reasons = _reasons(spark, [_trip("priority", pickup=None, distance=0.0, fare=-1.0)])
    assert reasons["priority"] == "null_pickup_or_dropoff"
