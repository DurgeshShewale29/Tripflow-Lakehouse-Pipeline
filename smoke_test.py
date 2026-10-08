"""Write a tiny Delta table and read it back."""

from importlib.metadata import version

from src.common.spark_session import get_spark

SMOKE_PATH = "data/smoke_delta"


def main() -> None:
    spark = get_spark()
    spark.range(5).write.format("delta").mode("overwrite").save(SMOKE_PATH)
    count = spark.read.format("delta").load(SMOKE_PATH).count()
    print(count)
    print(f"Spark {spark.version}")
    print(f"Delta {version('delta-spark')}")
    spark.stop()


if __name__ == "__main__":
    main()
