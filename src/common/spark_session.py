"""Local SparkSession with the Delta Lake extension and catalog."""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Point workers at this venv so Windows does not fall back to another Python.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark(app_name: str = "tripflow") -> SparkSession:
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

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
