"""Export a 2024-05 trip sample as JSON lines, standing in for an API feed."""

import logging

from pyspark.sql import functions as F

from src.common.config import EXTRA_MONTH_JSON, RAW_DIR, RAW_JSON_DIR
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

SAMPLE_ROWS = 300_000
JSON_TRIPS = RAW_JSON_DIR / f"trips_{EXTRA_MONTH_JSON}"
# Keep pickup timestamps as ISO text so the JSON file has no binary timestamps.
ISO_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"


def export_json_source(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        if JSON_TRIPS.exists():
            logger.info("skipping export; %s already exists", JSON_TRIPS)
            return
        source = RAW_DIR / f"yellow_tripdata_{EXTRA_MONTH_JSON}.parquet"
        if not source.is_file():
            raise FileNotFoundError(f"missing raw file: {source}")
        logger.info("writing first %s %s rows to %s", SAMPLE_ROWS, EXTRA_MONTH_JSON, JSON_TRIPS)
        sample = (
            spark.read.parquet(source.as_posix())
            .where(F.date_format("tpep_pickup_datetime", "yyyy-MM") == EXTRA_MONTH_JSON)
            .limit(SAMPLE_ROWS)
        )
        for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
            sample = sample.withColumn(name, F.date_format(F.col(name), ISO_TIMESTAMP))
        JSON_TRIPS.parent.mkdir(parents=True, exist_ok=True)
        sample.coalesce(1).write.mode("errorifexists").json(JSON_TRIPS.as_posix())
        logger.info("wrote %s", JSON_TRIPS)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    export_json_source()


if __name__ == "__main__":
    main()
