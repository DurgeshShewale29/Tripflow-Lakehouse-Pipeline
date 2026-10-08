"""Print schema, sample rows, and merge history for each Gold table."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import GOLD_DAILY_SUMMARY, GOLD_PAYMENT_BOROUGH, GOLD_REVENUE_ZONE_HOUR
from src.common.spark_session import get_spark

TABLES = (
    ("gold_revenue_by_zone_hour", GOLD_REVENUE_ZONE_HOUR),
    ("gold_daily_summary", GOLD_DAILY_SUMMARY),
    ("gold_payment_borough", GOLD_PAYMENT_BOROUGH),
)


def main() -> None:
    spark = get_spark()
    try:
        for name, path in TABLES:
            table_path = path.as_posix()
            frame = spark.read.format("delta").load(table_path)
            print(name)
            print("schema")
            frame.printSchema()
            print("sample")
            frame.show(5, truncate=False)
            print("history")
            (
                DeltaTable.forPath(spark, table_path)
                .history()
                .select(
                    "operation",
                    "timestamp",
                    F.col("operationMetrics").getItem("numTargetRowsInserted").alias("numTargetRowsInserted"),
                    F.col("operationMetrics").getItem("numTargetRowsUpdated").alias("numTargetRowsUpdated"),
                )
                .show(truncate=False)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
