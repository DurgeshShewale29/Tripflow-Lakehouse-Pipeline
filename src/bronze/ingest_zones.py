"""Overwrite the Bronze taxi-zone lookup Delta table."""

import logging

from pyspark.sql import functions as F

from src.common.config import BRONZE_ZONES, RAW_DIR
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

ZONE_FILE = "taxi_zone_lookup.csv"


def ingest_zones(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        source = RAW_DIR / ZONE_FILE
        if not source.is_file():
            raise FileNotFoundError(f"missing raw file: {source}")
        logger.info("ingesting %s", source.name)
        frame = spark.read.option("header", True).csv(source.as_posix())
        frame = frame.withColumn("ingest_ts", F.current_timestamp()).withColumn(
            "source_file", F.lit(source.name)
        )
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(BRONZE_ZONES.as_posix())
        )
        logger.info("wrote %s", BRONZE_ZONES.name)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ingest_zones()


if __name__ == "__main__":
    main()
