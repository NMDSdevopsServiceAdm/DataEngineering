# TODO Delete this file

import argparse
import sys

import pyspark.sql.functions as F
from pyspark.sql import Window

from utils import utils


def main(job_estimates_source, worker_source, output_destination=None):
    print("Determining job role breakdown for cqc locations")

    worker_df = get_worker_dataset(worker_source)
    job_estimate_df = get_job_estimates_dataset(job_estimates_source)
    worker_record_count_df = count_grouped_by_field(
        worker_df,
        grouping_field=["locationid", "snapshot_date"],
        alias="location_worker_records",
    )

    master_df = (
        job_estimate_df.join(
            worker_record_count_df,
            (job_estimate_df.master_locationid == worker_record_count_df.locationid)
            & (job_estimate_df.snapshot_date == worker_record_count_df.snapshot_date),
            "left",
        )
        .drop("locationid")
        .drop(worker_record_count_df.snapshot_date)
    )

    master_df = master_df.na.fill(value=0, subset=["location_worker_records"])

    master_df = get_comprehensive_list_of_job_roles_to_locations(worker_df, master_df)

    master_df = determine_worker_record_to_jobs_ratio(master_df)
    worker_record_per_location_count_df = count_grouped_by_field(
        worker_df,
        grouping_field=["locationid", "snapshot_date", "mainjrid"],
        alias="ascwds_num_of_jobs",
    )

    master_df = (
        master_df.join(
            worker_record_per_location_count_df,
            (
                worker_record_per_location_count_df.locationid
                == master_df.master_locationid
            )
            & (worker_record_per_location_count_df.mainjrid == master_df.main_job_role)
            & (
                worker_record_per_location_count_df.snapshot_date
                == master_df.snapshot_date
            ),
            "left",
        )
        .drop("locationid", "mainjrid")
        .drop(worker_record_per_location_count_df.snapshot_date)
    )

    master_df = master_df.na.fill(value=0, subset=["ascwds_num_of_jobs"])
    master_df = calculate_job_count_breakdown_by_service(master_df)

    master_df = calculate_job_count_breakdown_by_jobrole(master_df)

    print(f"Exporting as parquet to {output_destination}")
    if output_destination:
        utils.write_to_parquet(
            master_df,
            output_destination,
            mode="append",
            partitionKeys=["run_year", "run_month", "run_day"],
        )
    else:
        return master_df


def calculate_job_count_breakdown_by_jobrole(master_df):
    master_df = master_df.withColumn(
        "estimated_jobs_in_role",
        F.col("estimate_job_count") * F.col("estimated_job_role_percentage"),
    ).drop("estimated_job_role_percentage")

    master_df = master_df.withColumn(
        "estimated_minus_ascwds",
        F.greatest(
            F.lit(0), F.col("estimated_jobs_in_role") - F.col("ascwds_num_of_jobs")
        ),
    ).drop("estimated_jobs_in_role")

    master_df = master_df.withColumn(
        "sum_of_estimated_minus_ascwds",
        F.sum("estimated_minus_ascwds").over(Window.partitionBy("master_locationid")),
    )

    master_df = master_df.withColumn(
        "adjusted_job_role_percentage",
        F.coalesce(
            (F.col("estimated_minus_ascwds") / F.col("sum_of_estimated_minus_ascwds")),
            F.lit(0.0),
        ),
    ).drop("estimated_minus_ascwds", "sum_of_estimated_minus_ascwds")

    master_df = master_df.withColumn(
        "estimated_num_of_jobs",
        F.col("location_jobs_to_model") * F.col("adjusted_job_role_percentage"),
    ).drop("location_jobs_to_model", "adjusted_job_role_percentage")

    master_df = master_df.withColumn(
        "estimate_job_role_count",
        F.col("ascwds_num_of_jobs") + F.col("estimated_num_of_jobs"),
    ).drop("location_worker_records")

    return master_df


def calculate_job_count_breakdown_by_service(master_df):
    job_role_breakdown_by_service = (
        master_df.selectExpr(
            "primary_service_type AS service_type",
            "main_job_role AS job_role",
            "ascwds_num_of_jobs",
            "snapshot_date as breakdown_snapshot_date",
        )
        .groupBy("service_type", "job_role", "breakdown_snapshot_date")
        .agg(F.sum("ascwds_num_of_jobs").alias("ascwds_num_of_jobs_in_service"))
    )
    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "all_ascwds_jobs_in_service",
        F.sum("ascwds_num_of_jobs_in_service").over(
            Window.partitionBy("service_type", "breakdown_snapshot_date")
        ),
    )
    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "estimated_job_role_percentage",
        F.col("ascwds_num_of_jobs_in_service") / F.col("all_ascwds_jobs_in_service"),
    ).drop("ascwds_num_of_jobs_in_service", "all_ascwds_jobs_in_service")

    master_df = master_df.join(
        job_role_breakdown_by_service,
        (job_role_breakdown_by_service.service_type == master_df.primary_service_type)
        & (job_role_breakdown_by_service.job_role == master_df.main_job_role)
        & (
            job_role_breakdown_by_service.breakdown_snapshot_date
            == master_df.snapshot_date
        ),
        "left",
    ).drop("service_type", "job_role", "breakdown_snapshot_date")

    return master_df


def determine_worker_record_to_jobs_ratio(master_df):
    master_df = master_df.withColumn(
        "location_jobs_ratio",
        F.least(
            F.lit(1), F.col("estimate_job_count") / F.col("location_worker_records")
        ),
    )
    master_df = master_df.withColumn(
        "location_jobs_to_model",
        F.greatest(
            F.lit(0), F.col("estimate_job_count") - F.col("location_worker_records")
        ),
    )

    return master_df


def get_comprehensive_list_of_job_roles_to_locations(worker_df, master_df):
    unique_jobrole_df = get_distinct_list(worker_df, "mainjrid", alias="main_job_role")
    master_df = master_df.crossJoin(unique_jobrole_df)
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
    worker_df = spark.read.parquet(worker_source).select(
        F.col("locationid"), F.col("workerid"), F.col("mainjrid"), F.col("import_date")
    )

    worker_df = worker_df.withColumnRenamed("import_date", "snapshot_date")

    return worker_df


def get_job_estimates_dataset(job_estimates_source):
    spark = utils.get_spark()
    print(f"Reading job_estimates_2021 parquet from {job_estimates_source}")
    job_estimates_df = spark.read.parquet(job_estimates_source).select(
        F.col("locationid").alias("master_locationid"),
        F.col("primary_service_type"),
        F.col("estimate_job_count"),
        F.col("snapshot_date"),
        F.col("run_year"),
        F.col("run_month"),
        F.col("run_day"),
    )

    job_estimates_df = utils.filter_df_to_maximum_value_in_column(
        job_estimates_df, "snapshot_date"
    )

    job_estimates_df = job_estimates_df.withColumn(
        "snapshot_date", F.date_format(F.col("snapshot_date"), "yyyyMMdd")
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

    args, _ = parser.parse_known_args()

    return (
        args.job_estimates_source,
        args.worker_source,
        args.destination,
    )


if __name__ == "__main__":
    print("Spark job 'job_role_breakdown' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        job_estimates_source,
        worker_source,
        destination,
    ) = collect_arguments()

    main(job_estimates_source, worker_source, destination)

    print("Spark job 'job_role_breakdown' complete")
