"""Load a 2024-04 trip sample into PostgreSQL with Spark JDBC."""

import logging

from pyspark.sql import functions as F

from src.common.config import EXTRA_MONTH_PG, RAW_DIR
from src.common.postgres import jdbc_options
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)

TABLE = "public.yellow_trips_source"
SAMPLE_ROWS = 500_000


def _jdbc_reader(spark, dbtable: str):
    options = jdbc_options()
    return (
        spark.read.format("jdbc")
        .option("url", options["url"])
        .option("dbtable", dbtable)
        .option("user", options["user"])
        .option("password", options["password"])
        .option("driver", options["driver"])
        .option("fetchsize", "10000")
    )


def _table_has_rows(spark) -> bool:
    query = f"(SELECT COUNT(*) AS row_count FROM {TABLE}) AS counts"
    try:
        row_count = _jdbc_reader(spark, query).load().collect()[0]["row_count"]
    except Exception as exc:
        if "does not exist" not in str(exc).lower():
            raise
        logger.info("%s does not exist yet", TABLE)
        return False
    return int(row_count) > 0


def load_postgres_source(spark=None) -> None:
    own_spark = spark is None
    spark = spark or get_spark()
    try:
        if _table_has_rows(spark):
            logger.info("skipping load; %s already has rows", TABLE)
            return
        source = RAW_DIR / f"yellow_tripdata_{EXTRA_MONTH_PG}.parquet"
        if not source.is_file():
            raise FileNotFoundError(f"missing raw file: {source}")
        logger.info("writing first %s %s rows to %s", SAMPLE_ROWS, EXTRA_MONTH_PG, TABLE)
        sample = (
            spark.read.parquet(source.as_posix())
            .where(F.date_format("tpep_pickup_datetime", "yyyy-MM") == EXTRA_MONTH_PG)
            .limit(SAMPLE_ROWS)
        )
        options = jdbc_options()
        (
            sample.write.format("jdbc")
            .option("url", options["url"])
            .option("dbtable", TABLE)
            .option("user", options["user"])
            .option("password", options["password"])
            .option("driver", options["driver"])
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )
        logger.info("wrote %s", TABLE)
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    load_postgres_source()


if __name__ == "__main__":
    main()
