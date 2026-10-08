"""Benchmark a small-file Silver copy against a partitioned, Z-ordered copy."""

import json
import logging
import statistics
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import BRONZE_ZONES, PROJECT_ROOT, SILVER_TRIPS
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

BENCH_DIR = PROJECT_ROOT / "data" / "bench"
BASELINE_PATH = BENCH_DIR / "baseline"
OPTIMIZED_PATH = BENCH_DIR / "optimized"
RESULTS_MD = PROJECT_ROOT / "docs" / "benchmark_results.md"
RESULTS_JSON = PROJECT_ROOT / "docs" / "benchmark_results.json"
TIMED_RUNS = 3


def _load(spark, path):
    return spark.read.format("delta").load(path.as_posix())


def _execute(frame) -> None:
    frame.write.format("noop").mode("overwrite").save()


def _median_seconds(action) -> float:
    """Discard one warm-up run, then return the median of three timed runs."""
    action()
    samples = []
    for _ in range(TIMED_RUNS):
        started = time.perf_counter()
        action()
        samples.append(time.perf_counter() - started)
    return round(statistics.median(samples), 3)


def _table_stats(spark, path) -> dict:
    row = DeltaTable.forPath(spark, path.as_posix()).detail().collect()[0]
    stats = {"files": int(row["numFiles"]), "size_bytes": int(row["sizeInBytes"])}
    logger.info("%s files=%s size_bytes=%s", path.name, stats["files"], stats["size_bytes"])
    return stats


def _build_copies(spark) -> dict:
    silver = _load(spark, SILVER_TRIPS)
    logger.info("writing baseline copy repartitioned to 200 files")
    (
        silver.repartition(200)
        .write.format("delta")
        .mode("overwrite")
        .save(BASELINE_PATH.as_posix())
    )
    baseline = _table_stats(spark, BASELINE_PATH)

    logger.info("writing optimized copy partitioned by source_month")
    (
        silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("source_month")
        .save(OPTIMIZED_PATH.as_posix())
    )
    (
        DeltaTable.forPath(spark, OPTIMIZED_PATH.as_posix())
        .optimize()
        .executeZOrderBy("pu_location_id", "pickup_date")
    )
    optimized = _table_stats(spark, OPTIMIZED_PATH)
    return {"baseline": baseline, "optimized": optimized}


def _q1(trips):
    return (
        trips.where(F.col("source_month") == "2024-03")
        .groupBy("pu_location_id", "pickup_hour")
        .agg(F.sum("total_amount").alias("revenue"), F.count("*").alias("trips"))
    )


def _q2(trips):
    return trips.where(
        (F.col("pu_location_id") == 132)
        & F.col("pickup_date").between(F.lit("2024-03-01").cast("date"), F.lit("2024-03-07").cast("date"))
    ).agg(F.sum("total_amount").alias("total_amount"))


def _zones(spark):
    return _load(spark, BRONZE_ZONES).select(
        F.col("LocationID").cast("int").alias("pu_location_id"),
        F.col("Borough").alias("borough"),
    )


def _q3(trips, zones, broadcast: bool):
    # autoBroadcastJoinThreshold is -1, so only the explicit hint broadcasts.
    matched = trips.join(F.broadcast(zones) if broadcast else zones, "pu_location_id")
    return matched.groupBy("borough").agg(F.sum("total_amount").alias("total_amount"))


def _result_row(query: str, baseline_s: float, optimized_s: float) -> dict:
    speedup = round(baseline_s / optimized_s, 2) if optimized_s else None
    return {
        "query": query,
        "baseline_s": baseline_s,
        "optimized_s": optimized_s,
        "speedup_x": speedup,
    }


def _time_layout_query(spark, query_name: str, query) -> dict:
    logger.info("timing %s", query_name)
    spark.catalog.clearCache()
    baseline_s = _median_seconds(lambda: _execute(query(_load(spark, BASELINE_PATH))))
    spark.catalog.clearCache()
    optimized_s = _median_seconds(lambda: _execute(query(_load(spark, OPTIMIZED_PATH))))
    return _result_row(query_name, baseline_s, optimized_s)


def _time_q3(spark) -> dict:
    logger.info("timing Q3 on the optimized copy")
    zones = _zones(spark)
    spark.catalog.clearCache()
    no_broadcast_s = _median_seconds(
        lambda: _execute(_q3(_load(spark, OPTIMIZED_PATH), zones, broadcast=False))
    )
    spark.catalog.clearCache()
    broadcast_s = _median_seconds(
        lambda: _execute(_q3(_load(spark, OPTIMIZED_PATH), zones, broadcast=True))
    )
    # Same columns as the layout queries: no-broadcast vs broadcast.
    return _result_row("Q3", no_broadcast_s, broadcast_s)


def _write_results(storage: dict, queries: list[dict]) -> None:
    total_baseline = round(sum(row["baseline_s"] for row in queries), 3)
    total_optimized = round(sum(row["optimized_s"] for row in queries), 3)
    q3_note = (
        "Q3 uses the optimized copy for both timings. "
        "baseline_s is the join with no broadcast hint; optimized_s is broadcast(zones)."
    )
    lines = [
        "# Silver layout benchmark",
        "",
        "## Files",
        "",
        "| copy | files | size_bytes |",
        "| --- | ---: | ---: |",
        f"| baseline | {storage['baseline']['files']} | {storage['baseline']['size_bytes']} |",
        f"| optimized | {storage['optimized']['files']} | {storage['optimized']['size_bytes']} |",
        "",
        "## Queries",
        "",
        q3_note,
        "",
        "| query | baseline_s | optimized_s | speedup_x |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in queries:
        lines.append(
            f"| {row['query']} | {row['baseline_s']:.3f} | {row['optimized_s']:.3f} | {row['speedup_x']} |"
        )
    lines.extend(
        [
            "",
            f"Total baseline: {total_baseline:.3f} s",
            "",
            f"Total optimized: {total_optimized:.3f} s",
            "",
        ]
    )
    RESULTS_MD.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_MD.write_text("\n".join(lines), encoding="utf-8")
    payload = {
        "storage": storage,
        "queries": queries,
        "total_baseline_s": total_baseline,
        "total_optimized_s": total_optimized,
        "notes": {"Q3": q3_note},
    }
    RESULTS_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    logger.info("copy files size_bytes")
    for name, stats in storage.items():
        logger.info("%s %s %s", name, stats["files"], stats["size_bytes"])
    logger.info("query baseline_s optimized_s speedup_x")
    for row in queries:
        logger.info("%s %s %s %s", row["query"], row["baseline_s"], row["optimized_s"], row["speedup_x"])
    logger.info("total baseline_s=%s optimized_s=%s", total_baseline, total_optimized)


def benchmark(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        storage = _build_copies(spark)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        queries = [
            _time_layout_query(spark, "Q1", _q1),
            _time_layout_query(spark, "Q2", _q2),
            _time_q3(spark),
        ]
        _write_results(storage, queries)
        logger.info("wrote %s and %s", RESULTS_MD.name, RESULTS_JSON.name)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    benchmark()


if __name__ == "__main__":
    main()
