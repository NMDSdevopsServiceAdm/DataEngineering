import argparse
from pyspark.sql.functions import col, lit, least, greatest, sum, coalesce
from pyspark.sql import Window
from utils import utils


def main(job_estimates_source, worker_source, output_destination=None):
    print("Determining job role breakdown for cqc locations")

    worker_df = get_worker_dataset(worker_source)
    job_estimate_df = get_job_estimates_dataset(job_estimates_source)
    worker_record_count_df = count_grouped_by_field(
        worker_df, grouping_field="locationid", alias="location_worker_records"
    )

    master_df = job_estimate_df.join(
        worker_record_count_df,
        job_estimate_df.master_locationid == worker_record_count_df.locationid,
        "left",
    ).drop("locationid")

    master_df = master_df.na.fill(value=0, subset=["location_worker_records"])

    master_df = get_comprehensive_list_of_job_roles_to_locations(worker_df, master_df)

    master_df = determine_worker_record_to_jobs_ratio(master_df)

    worker_record_per_location_count_df = count_grouped_by_field(
        worker_df, grouping_field=["locationid", "mainjrid"], alias="ascwds_num_of_jobs"
    )

    master_df = master_df.join(
        worker_record_per_location_count_df,
        (worker_record_per_location_count_df.locationid == master_df.master_locationid)
        & (worker_record_per_location_count_df.mainjrid == master_df.main_job_role),
        "left",
    ).drop("locationid", "mainjrid")

    master_df = master_df.na.fill(value=0, subset=["ascwds_num_of_jobs"])
    master_df = calculate_job_count_breakdown_by_service(master_df)
    master_df = calculate_job_count_breakdown_by_jobrole(master_df)

    print(f"Exporting as parquet to {output_destination}")
    if output_destination:
        utils.write_to_parquet(master_df, output_destination)
    else:
        return master_df


def calculate_job_count_breakdown_by_jobrole(master_df):

    master_df = master_df.withColumn(
        "estimated_jobs_in_role",
        col("estimate_job_count_2021") * col("estimated_job_role_percentage"),
    ).drop("estimated_job_role_percentage")

    master_df = master_df.withColumn(
        "estimated_minus_ascwds",
        greatest(lit(0), col("estimated_jobs_in_role") - col("ascwds_num_of_jobs")),
    ).drop("estimated_jobs_in_role")

    master_df = master_df.withColumn(
        "sum_of_estimated_minus_ascwds",
        sum("estimated_minus_ascwds").over(Window.partitionBy("master_locationid")),
    )

    master_df = master_df.withColumn(
        "adjusted_job_role_percentage",
        coalesce(
            (col("estimated_minus_ascwds") / col("sum_of_estimated_minus_ascwds")),
            lit(0.0),
        ),
    ).drop("estimated_minus_ascwds", "sum_of_estimated_minus_ascwds")

    master_df = master_df.withColumn(
        "estimated_num_of_jobs",
        col("location_jobs_to_model") * col("adjusted_job_role_percentage"),
    ).drop("location_jobs_to_model", "adjusted_job_role_percentage")

    master_df = master_df.withColumn(
        "estimate_job_role_count_2021",
        col("ascwds_num_of_jobs") + col("estimated_num_of_jobs"),
    ).drop("location_worker_records")

    return master_df


def calculate_job_count_breakdown_by_service(master_df):
    job_role_breakdown_by_service = (
        master_df.selectExpr(
            "primary_service_type AS service_type",
            "main_job_role AS job_role",
            "ascwds_num_of_jobs",
        )
        .groupBy("service_type", "job_role")
        .agg(sum("ascwds_num_of_jobs").alias("ascwds_num_of_jobs_in_service"))
    )
    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "all_ascwds_jobs_in_service",
        sum("ascwds_num_of_jobs_in_service").over(Window.partitionBy("service_type")),
    )
    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "estimated_job_role_percentage",
        col("ascwds_num_of_jobs_in_service") / col("all_ascwds_jobs_in_service"),
    ).drop("ascwds_num_of_jobs_in_service", "all_ascwds_jobs_in_service")

    master_df = master_df.join(
        job_role_breakdown_by_service,
        (job_role_breakdown_by_service.service_type == master_df.primary_service_type)
        & (job_role_breakdown_by_service.job_role == master_df.main_job_role),
        "left",
    ).drop("service_type", "job_role")

    return master_df


def determine_worker_record_to_jobs_ratio(master_df):
    master_df = master_df.withColumn(
        "location_jobs_ratio",
        least(lit(1), col("estimate_job_count_2021") / col("location_worker_records")),
    )
    master_df = master_df.withColumn(
        "location_jobs_to_model",
        greatest(
            lit(0), col("estimate_job_count_2021") - col("location_worker_records")
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
        col("locationid"), col("workerid"), col("mainjrid")
    )

    # GARY - THINK WE NEED TO FILTER THIS TO ONE IMPORT_DATE (20210331 in jupyter)

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
