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

    worker_record_count = worker_df.select("locationid").groupBy(
        "locationid").agg(count("locationid").alias("location_worker_record_count"))

    master_df = job_estimate_df.join(
        worker_record_count, "locationid")

    unique_jobrole_df = worker_df.selectExpr(
        "mainjrid AS main_job_role_id").distinct()

    # Create a mapping of every job role to every location.
    master_df = master_df.crossJoin(unique_jobrole_df)

    # Prepare location fields

    master_df = master_df.withColumn("location_jobs_ratio", least(
        lit(1), col("estimate_job_count_2021")/col("location_worker_record_count")))
    master_df = master_df.withColumn("location_jobs_to_model", greatest(
        lit(0), col("estimate_job_count_2021")-col("location_worker_record_count")))

    # Remove worker id, aggregate count by jobrole and location
    worker_df = worker_df.groupby('locationid', 'mainjrid').count(
    ).withColumnRenamed("count", "ascwds_num_of_jobs")

    master_df = master_df.join(worker_df, (worker_df.locationid == master_df.locationid) & (
        worker_df.mainjrid == master_df.main_job_role_id), 'left').drop('locationid', 'mainjrid')

    master_df = master_df.na.fill(value=0, subset=["ascwds_num_of_jobs"])

    # ---- -----

    master_df = determine_job_role_breakdown_by_service(master_df)

    # estimated jobs in each role for estimated jobs
    master_df = master_df.withColumn("estimated_jobs_in_role", col(
        "estimate_job_count_2021") * col("estimated_job_role_percentage")).drop("estimated_job_role_percentage")

    # compare estimated jobs to ascwds
    master_df = master_df.withColumn("estimated_minus_ascwds", greatest(lit(0), col(
        "estimated_jobs_in_role")-col("ascwds_num_of_jobs"))).drop("estimated_jobs_in_role")

    master_df = master_df.withColumn("sum_of_estimated_minus_ascwds", sum(
        "estimated_minus_ascwds").over(Window.partitionBy("locationid")))

    master_df = master_df.withColumn("adjusted_job_role_percentage", col("estimated_minus_ascwds")/col(
        "sum_of_estimated_minus_ascwds")).drop("estimated_minus_ascwds", "sum_of_estimated_minus_ascwds")

    master_df = master_df.withColumn("estimated_num_of_jobs", col("location_jobs_to_model") * col(
        "adjusted_job_role_percentage")).drop("location_jobs_to_model", "adjusted_job_role_percentage")

    master_df = master_df.withColumn("estimate_job_role_count_2021", col(
        "ascwds_num_of_jobs") + col("estimated_num_of_jobs")).drop("location_worker_record_count")

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(master_df, destination)


def determine_job_role_breakdown_by_service(df):
    job_role_breakdown_by_service = df.selectExpr("primary_service_type AS service_type", "main_job_role_id AS job_role", "ascwds_num_of_jobs").groupBy(
        "service_type", "job_role").agg(sum("ascwds_num_of_jobs").alias("ascwds_num_of_jobs_in_service"))

    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "all_ascwds_jobs_in_service", sum("ascwds_num_of_jobs_in_service").over(Window.partitionBy("service_type")))

    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn("estimated_job_role_percentage", col(
        "ascwds_num_of_jobs_in_service")/col("all_ascwds_jobs_in_service")).drop("ascwds_num_of_jobs_in_service", "all_ascwds_jobs_in_service")

    output_df = df.join(job_role_breakdown_by_service, (job_role_breakdown_by_service.service_type == df.primary_service_type) & (
        job_role_breakdown_by_service.job_role == df.main_job_role_id), 'left').drop('service_type', 'job_role')

    return output_df


def get_worker_dataset(worker_source):
    spark = utils.get_spark()
    print(f"Reading worker source parquet from {worker_source}")
    worker_df = (
        spark.read
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
        job_estimates_source,
        worker_source,
        destination,
    ) = collect_arguments()
    main(job_estimates_source, worker_source, destination)
