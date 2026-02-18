# conftest.py
from typing import cast

import pydeequ
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
# worked_id fixture comes from pytest-xdist
def spark(worker_id):
    """
    Creates a unique SparkSession for each xdist worker.
    The 'worker_id' fixture is provided by pytest-xdist (e.g., 'gw0', 'gw1').
    """
    # Cast type here to keep pylance happy.
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = (
        builder.master("local[1]")  # Give 1 thread to each worker
        .appName(f"spark-test-{worker_id}")
        # KEY: Isolate the Derby metastore and Warehouse to prevent file locks
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home=/tmp/derby/{worker_id}",
        )
        .config("spark.sql.warehouse.dir", f"/tmp/spark-warehouse/{worker_id}")
        # KEY: Randomize UI port to avoid 'Address already in use'
        .config("spark.ui.port", "0")
        # Optimization: set shuffle partitions to 1 for speed
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )

    spark.sql("set spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY")
    spark.sql("set spark.sql.parquet.datetimeRebaseModeInRead=LEGACY")
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    yield spark
    spark.stop()
