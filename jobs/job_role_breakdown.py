# TODO Delete this file

import sys

from pyspark.sql import DataFrame, functions as F, Window

from utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
cleaned_ascwds_worker_columns_to_import = [
    AWKClean.ascwds_worker_import_date,
    AWKClean.establishment_id,
    AWKClean.worker_id,
    AWKClean.main_job_role_clean_labelled,
]
estimated_ind_cqc_filled_posts_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_gac_service_types,
    IndCQC.imputed_registration_date,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    estimated_ind_cqc_filled_posts_source: str,
    cleaned_ascwds_worker_source: str,
    estimated_ind_cqc_filled_posts_by_job_role_destination: str,
):
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimated_ind_cqc_filled_posts_source (str): path to the estimates ind cqc filled posts data
        cleaned_ascwds_worker_source (str): path to the cleaned worker data
        estimated_ind_cqc_filled_posts_by_job_role_destination (str): path to where to save the outputs
    """
    estimated_posts_df = utils.read_from_parquet(
        estimated_ind_cqc_filled_posts_source,
        selected_columns=estimated_ind_cqc_filled_posts_columns_to_import,
    )
    cleaned_workers_df = utils.read_from_parquet(
        cleaned_ascwds_worker_source,
        selected_columns=cleaned_ascwds_worker_columns_to_import,
    )

    worker_counts_df = count_workers_by_location(cleaned_workers_df)
    master_df = merge_dataframes(estimated_posts_df, worker_counts_df)

    job_roles_df = get_unique_job_roles(cleaned_workers_df)
    master_df = master_df.crossJoin(job_roles_df)

    master_df = calculate_ratios(master_df)
    job_counts_df = count_jobs_by_location_and_role(cleaned_workers_df)
    master_df = master_df.join(
        job_counts_df,
        [
            AWKClean.establishment_id,
            AWKClean.main_job_role_clean_labelled,
            AWKClean.ascwds_worker_import_date,
        ],
        "left",
    ).fillna(0)

    master_df = estimate_job_counts(master_df)
    utils.write_to_parquet(
        master_df, estimated_ind_cqc_filled_posts_by_job_role_destination, "overwrite"
    )


def count_workers_by_location(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(AWKClean.establishment_id, AWKClean.ascwds_worker_import_date)
        .count()
        .withColumnRenamed("count", "worker_count")
    )


def merge_dataframes(posts_df: DataFrame, workers_df: DataFrame) -> DataFrame:
    posts_df = (
        posts_df.join(
            workers_df,
            (posts_df[IndCQC.establishment_id] == workers_df[AWKClean.establishment_id])
            & (
                posts_df[IndCQC.ascwds_workplace_import_date]
                == workers_df[AWKClean.ascwds_worker_import_date]
            ),
            "left",
        )
        .drop(workers_df.establishmentid)
        .fillna(0, "worker_count")
    )

    return posts_df


def get_unique_job_roles(df: DataFrame) -> DataFrame:
    return df.select(AWKClean.main_job_role_clean_labelled).distinct()


def calculate_ratios(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "jobs_ratio",
        F.least(F.lit(1), F.col(IndCQC.estimate_filled_posts) / F.col("worker_count")),
    )
    df = df.withColumn(
        "jobs_to_model",
        F.greatest(
            F.lit(0), F.col(IndCQC.estimate_filled_posts) - F.col("worker_count")
        ),
    )
    return df


def count_jobs_by_location_and_role(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(
            AWKClean.establishment_id,
            AWKClean.ascwds_worker_import_date,
            AWKClean.main_job_role_clean_labelled,
        )
        .count()
        .withColumnRenamed("count", "job_count")
    )


def estimate_job_counts(df: DataFrame) -> DataFrame:
    primary_service_window_spec = Window.partitionBy(
        IndCQC.primary_service_type, IndCQC.cqc_location_import_date
    )
    df = df.withColumn(
        "total_jobs_in_service", F.sum("job_count").over(primary_service_window_spec)
    )
    primary_service_job_role_window_spec = Window.partitionBy(
        IndCQC.primary_service_type,
        IndCQC.cqc_location_import_date,
        AWKClean.main_job_role_clean_labelled,
    )
    df = df.withColumn(
        "total_job_roles_in_service",
        F.sum("job_count").over(primary_service_job_role_window_spec),
    )
    df = df.withColumn(
        "job_role_percentage",
        F.col("total_job_roles_in_service") / F.col("total_jobs_in_service"),
    )
    df = df.withColumn(
        "estimated_jobs",
        F.col(IndCQC.estimate_filled_posts) * F.col("job_role_percentage"),
    )
    df = df.withColumn(
        "adjusted_jobs",
        F.greatest(F.lit(0), F.col("estimated_jobs") - F.col("job_count")),
    )
    df = df.withColumn(
        "sum_adjusted_jobs",
        F.sum("adjusted_jobs").over(Window.partitionBy(IndCQC.location_id)),
    )
    df = df.withColumn(
        "final_job_percentage",
        F.coalesce(F.col("adjusted_jobs") / F.col("sum_adjusted_jobs"), F.lit(0)),
    )
    df = df.withColumn(
        "final_estimated_jobs", F.col("jobs_to_model") * F.col("final_job_percentage")
    )
    df = df.withColumn("rebased_jobs", F.col("job_count") * F.col("jobs_ratio"))
    df = df.withColumn(
        "estimate_filled_posts_by_job_role",
        F.col("rebased_jobs") + F.col("final_estimated_jobs"),
    )
    return df.drop(
        "worker_count",
        "jobs_ratio",
        "jobs_to_model",
        "job_count",
        "total_jobs_in_service",
        "total_job_roles_in_service",
        "job_role_percentage",
        "estimated_jobs",
        "adjusted_jobs",
        "sum_adjusted_jobs",
        "final_job_percentage",
        "final_estimated_jobs",
        "rebased_jobs",
    )


if __name__ == "__main__":
    print("spark job: estimate_ind_cqc_filled_posts_by_job_role starting")
    print(f"job args: {sys.argv}")

    (
        estimated_ind_cqc_filled_posts_source,
        cleaned_ascwds_worker_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
    ) = utils.collect_arguments(
        (
            "--estimated_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for parquet ASCWDS worker cleaned dataset",
        ),
        (
            "--estimated_ind_cqc_filled_posts_by_job_role_destination",
            "Destination s3 directory",
        ),
    )

    main(
        estimated_ind_cqc_filled_posts_source,
        cleaned_ascwds_worker_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
    )
