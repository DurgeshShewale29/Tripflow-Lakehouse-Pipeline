"""Download raw files, then load Bronze zones and trips."""

import logging

from src.bronze.download_data import download_data
from src.bronze.ingest_trips import ingest_trips
from src.bronze.ingest_zones import ingest_zones
from src.common.config import BRONZE_TRIPS
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)


def _log_month_counts(spark) -> None:
    rows = (
        spark.read.format("delta")
        .load(BRONZE_TRIPS.as_posix())
        .groupBy("source_month")
        .count()
        .orderBy("source_month")
        .collect()
    )
    total = 0
    for row in rows:
        logger.info("source_month=%s rows=%s", row.source_month, row["count"])
        total += row["count"]
    logger.info("total_rows=%s", total)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    download_data()
    spark = get_spark()
    try:
        ingest_zones(spark)
        ingest_trips(spark)
        _log_month_counts(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
