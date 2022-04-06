"""
Return a list of all cqc locations with a breakdown of job role counts for 2021

Columns: Locationid, jobroleid, jobrole name, count of workers
"""

import argparse
from pyspark.sql.functions import col, count, lit, least, greatest, sum, round
from pyspark.sql import Window
from utils import utils


def main(job_estimates_source, worker_source, destinaton):
    print("Determining job role breakdown for cqc locations")

    worker_df = get_worker_dataset(worker_source)
    job_estimate_df = get_job_estimates_dataset(job_estimates_source)
    master_df = None

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(master_df, destination)


def get_worker_dataset(worker_source):
    spark = utils.get_spark()
    print(f"Reading worker source parquet from {worker_source}")
    worker_df = (
        spark.read
        .parquet(worker_source)
        .select(
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
            col("locationid").alias("master_locationid"),
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
        job_estimates_source,
        worker_source,
        destination,
    ) = collect_arguments()
    main(job_estimates_source, worker_source, destination)
