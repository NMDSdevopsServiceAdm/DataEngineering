# scripts/fetch_dependencies.py
from typing import cast

import pydeequ
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Fetching PyDeequ dependencies...")
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = (
        builder.appName("dependency-fetcher")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        # Use a tiny memory footprint for the fetcher
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    print("Dependencies downloaded successfully.")
    spark.stop()
