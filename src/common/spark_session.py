"""Local SparkSession with the Delta Lake extension and catalog."""

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Point workers at this venv so Windows does not fall back to another Python.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark(app_name: str = "tripflow", *, testing: bool | None = None) -> SparkSession:
    # Prefer the local JDK and Hadoop install when they are present.
    if os.name == "nt":
        java_home = r"C:\Program Files\Java\jdk-21.0.12.1"
        hadoop_home = r"C:\hadoop"
        if os.path.isdir(java_home):
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")
        if os.path.isdir(hadoop_home):
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    spark_tmp = Path(__file__).resolve().parents[2] / ".spark_tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)

    # Tests pass testing=True, or set TRIPFLOW_SPARK_TESTING=1, for a small session.
    if testing is None:
        testing = os.environ.get("TRIPFLOW_SPARK_TESTING") == "1"
    master = "local[2]" if testing else "local[*]"
    driver_memory = "1g" if testing else "4g"
    shuffle_partitions = "2" if testing else "8"

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(spark_tmp))
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
