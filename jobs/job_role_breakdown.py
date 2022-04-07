"""
Return a list of all cqc locations with a breakdown of job role counts for 2021

Columns: Locationid, jobroleid, jobrole name, count of workers
"""

import argparse
from pyspark.sql.functions import col, count, lit, least, greatest, sum, round
from pyspark.sql import Window
from utils import utils


def main(job_estimates_source, worker_source, output_destination=None):
    print("Determining job role breakdown for cqc locations")

    worker_df = get_worker_dataset(worker_source)  # MASTER DF IN JUPYTER
    job_estimate_df = get_job_estimates_dataset(job_estimates_source)
    worker_record_count_df = count_grouped_by_field(
        worker_df, grouping_field="locationid", alias="location_worker_records"
    )

    master_df = job_estimate_df.join(
        worker_record_count_df, job_estimate_df.master_locationid == worker_record_count_df.locationid
    ).drop("locationid")

    unique_jobrole_df = get_distinct_list(worker_df, "mainjrid", alias="main_job_role")
    master_df = master_df.crossJoin(unique_jobrole_df)

    # Currently at step 11 - (# Collect unique job roles)

    print(f"Exporting as parquet to {output_destination}")
    if output_destination:
        utils.write_to_parquet(master_df, output_destination)
    else:
        return master_df


def get_distinct_list(input_df, column_name, alias=None):

    output_df = input_df.select(column_name).distinct()

    if alias:
        output_df = output_df.withColumnRenamed(column_name, alias)

    return output_df


def count_grouped_by_field(input_df, grouping_field="locationid", alias=None):

    output_df = input_df.select(grouping_field).groupBy(grouping_field).count()

    if alias:
        output_df = output_df.withColumnRenamed("count", alias)

    return output_df


def get_worker_dataset(worker_source):
    spark = utils.get_spark()
    print(f"Reading worker source parquet from {worker_source}")
    worker_df = spark.read.parquet(worker_source).select(col("locationid"), col("workerid"), col("mainjrid"))

    return worker_df


def get_job_estimates_dataset(job_estimates_source):
    spark = utils.get_spark()
    print(f"Reading job_estimates_2021 parquet from {job_estimates_source}")
    job_estimates_df = spark.read.parquet(job_estimates_source).select(
        col("locationid").alias("master_locationid"),
        col("primary_service_type"),
        col("estimate_job_count_2021"),
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
