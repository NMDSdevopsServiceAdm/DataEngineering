# conftest.py
import os
from typing import cast

import psutil
import pytest
from pyspark.sql import SparkSession


def pytest_sessionstart(session):
    """Hook to run before the session starts - set env vars here."""
    os.environ["SPARK_VERSION"] = "3.5"


@pytest.fixture(scope="session")
# worked_id fixture comes from pytest-xdist
def spark(worker_id):
    """
    Creates a unique SparkSession for each xdist worker.

    The 'worker_id' fixture is provided by pytest-xdist (e.g., 'gw0', 'gw1').
    Still works fine when xdist isn't used.
    """
    # Import in here because it needs the SPARK_VERSION env var to init.
    import pydeequ

    # Calculate a safe cap per worker (e.g., 1GB)
    mem_limit = "1g"
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
        .config("spark.driver.memory", mem_limit)  # Cap the individual JVM
        .config("spark.executor.memory", mem_limit)
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )

    spark.sql("set spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY")
    spark.sql("set spark.sql.parquet.datetimeRebaseModeInRead=LEGACY")
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    yield spark
    spark.stop()


def pytest_xdist_auto_num_workers(config):
    """
    Logic to prevent OOM when running multiple Spark JVMs.

    Each Spark worker needs roughly 1.5GB of overhead.
    """
    cpus = os.cpu_count() or 1
    # Get available RAM in GB
    available_gb = psutil.virtual_memory().available / (1024**3)

    # Estimate: Allow 1 worker per 1.5GB of available RAM
    mem_based_workers = int(available_gb // 1.5)

    # 3. Apply a "Diminishing Returns" Cap
    # For Spark, 4-6 workers is usually the peak efficiency for local I/O
    MAX_WORKERS_CAP = 4

    optimal_workers = max(1, min(cpus, mem_based_workers, MAX_WORKERS_CAP))

    print(f"\n[Resource Guard] CPUs: {cpus}, RAM: {available_gb:.1f}GB")
    print(f"[Resource Guard] Scaling to {optimal_workers} workers to stay safe.")

    return optimal_workers
