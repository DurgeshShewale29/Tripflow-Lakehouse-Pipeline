"""Print schema, sample rows, monthly counts, and history for Bronze trips."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from delta.tables import DeltaTable

from src.common.config import BRONZE_TRIPS
from src.common.spark_session import get_spark

AUDIT_COLUMNS = ["ingest_ts", "source_file", "source_month", "batch_id"]


def main() -> None:
    spark = get_spark()
    try:
        path = BRONZE_TRIPS.as_posix()
        trips = spark.read.format("delta").load(path)

        print("schema")
        trips.printSchema()

        # Keep the Bronze audit columns visible at the front of each sample row.
        ordered = AUDIT_COLUMNS + [column for column in trips.columns if column not in AUDIT_COLUMNS]
        print("sample")
        trips.select(*ordered).show(5, truncate=False)

        print("rows per source_month")
        trips.groupBy("source_month").count().orderBy("source_month").show(truncate=False)

        print("history")
        DeltaTable.forPath(spark, path).history().select("operation", "timestamp").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
