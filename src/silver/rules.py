"""Bronze quality rules. The first failing rule sets reject_reason."""

from pyspark.sql import DataFrame, functions as F


def with_reject_reason(trips: DataFrame, zones: DataFrame | list[int]) -> DataFrame:
    """Return Bronze trips with reject_reason set, or null when the row is valid."""
    if not isinstance(zones, DataFrame):
        zones = trips.sparkSession.createDataFrame(
            [(int(zone_id),) for zone_id in zones],
            "LocationID int",
        )
    location_ids = (
        zones.select(F.col("LocationID").cast("int").alias("location_id"))
        .where(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
    )
    pickup_known = location_ids.select(
        F.col("location_id").alias("PULocationID"),
        F.lit(True).alias("_pickup_zone_known"),
    )
    dropoff_known = location_ids.select(
        F.col("location_id").alias("DOLocationID"),
        F.lit(True).alias("_dropoff_zone_known"),
    )
    flagged = trips.join(F.broadcast(pickup_known), "PULocationID", "left").join(
        F.broadcast(dropoff_known), "DOLocationID", "left"
    )

    null_pickup_or_dropoff = F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull()
    dropoff_not_after_pickup = F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")
    pickup_outside_source_month = F.date_format("tpep_pickup_datetime", "yyyy-MM") != F.col("source_month")
    duration_over_24h = F.expr(
        "timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) > 86400"
    )
    invalid_distance = (
        F.col("trip_distance").isNull()
        | (F.col("trip_distance") <= 0)
        | (F.col("trip_distance") > 200)
    )
    negative_fare_or_total = (F.col("fare_amount") < 0) | (F.col("total_amount") < 0)
    invalid_passenger_count = F.col("passenger_count").isNotNull() & (
        (F.col("passenger_count") < 0) | (F.col("passenger_count") > 9)
    )
    unknown_pickup_zone = F.col("_pickup_zone_known").isNull()
    unknown_dropoff_zone = F.col("_dropoff_zone_known").isNull()

    reject_reason = (
        F.when(null_pickup_or_dropoff, "null_pickup_or_dropoff")
        .when(dropoff_not_after_pickup, "dropoff_not_after_pickup")
        .when(pickup_outside_source_month, "pickup_outside_source_month")
        .when(duration_over_24h, "duration_over_24h")
        .when(invalid_distance, "invalid_distance")
        .when(negative_fare_or_total, "negative_fare_or_total")
        .when(invalid_passenger_count, "invalid_passenger_count")
        .when(unknown_pickup_zone, "unknown_pickup_zone")
        .when(unknown_dropoff_zone, "unknown_dropoff_zone")
    )
    return flagged.withColumn("reject_reason", reject_reason).drop(
        "_pickup_zone_known", "_dropoff_zone_known"
    )
