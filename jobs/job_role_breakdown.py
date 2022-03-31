"""
Return a list of all cqc locations with a breakdown of job role counts for 2021

Columns: Locationid, jobroleid, jobrole name, count of workers
"""

import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col, to_date, lower
from pyspark.sql.types import IntegerType
import datetime
from pyspark.sql.functions import col, least, greatest, lit, sum, countDistinct, expr, count, round
from pyspark.sql import Window
from utils import utils


def main(worker_source, job_estimates_source, destinaton):
    print("Determining job role breakdown for cqc locations")
    output_df = None
    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(output_df, destination)


def get_worker_dataset(worker_source, base_path):
    spark = utils.get_spark()
    print(f"Reading worksources parquet from {worker_source}")
    worker_df = (
        spark.read.option("basePath", base_path)
        .parquet(worker_source)
        .select(
            col("establishmentid"),
            col("locationid"),
            col("workerid"),
            col("mainjrid")
        )
    )

    return worker_df


def get_job_estimates_dataset(job_estimates_source):
    spark = utils.get_spark()
    print(f"Reading job_estimates_2021 parquet from {job_estimates_source}")
    job_estimates_df = (
        spark.read
        .parquet(job_estimates_source)
        .select(
            col("locationid"),
            col("primary_service_type"),
            col("estimate_job_count_2021"),
        )
    )

    return job_estimates_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--job_estimates_source",
        help="Location of dataset_job_estimates_2021",
        required=True,
    )
    parser.add_argument(
        "--worker_source",
        help="Location of dataset_worker",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting job role breakdown",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.job_estimates_source,
        args.worker_source,
        args.destination,
    )


if __name__ == "__main__":
    (
        worker_source,
        job_estimates_source,
        destination,
    ) = collect_arguments()
    main(worker_source, job_estimates_source, destination)
