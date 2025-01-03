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
    estimated_ind_cqc_filled_posts_df = utils.read_from_parquet(
        estimated_ind_cqc_filled_posts_source,
        selected_columns=estimated_ind_cqc_filled_posts_columns_to_import,
    )
    cleaned_ascwds_worker_df = utils.read_from_parquet(
        cleaned_ascwds_worker_source,
        selected_columns=cleaned_ascwds_worker_columns_to_import,
    )

    # old process
    worker_record_count_df = count_grouped_by_field(
        cleaned_ascwds_worker_df,
        grouping_field=[AWKClean.establishment_id, AWKClean.ascwds_worker_import_date],
        alias="location_worker_records",
    )
    # worker_record_count_df.show()

    master_df = estimated_ind_cqc_filled_posts_df.join(
        worker_record_count_df,
        (
            estimated_ind_cqc_filled_posts_df[IndCQC.establishment_id]
            == worker_record_count_df[AWKClean.establishment_id]
        )
        & (
            estimated_ind_cqc_filled_posts_df[IndCQC.ascwds_workplace_import_date]
            == worker_record_count_df[AWKClean.ascwds_worker_import_date]
        ),
        "left",
    ).drop(worker_record_count_df.establishmentid)
    # master_df.show()

    master_df = master_df.na.fill(value=0, subset=["location_worker_records"])

    master_df = get_comprehensive_list_of_job_roles_to_locations(
        cleaned_ascwds_worker_df, master_df
    )
    # master_df.show()

    master_df = determine_worker_record_to_jobs_ratio(master_df)
    # master_df.show()

    worker_record_per_location_count_df = count_grouped_by_field(
        cleaned_ascwds_worker_df,
        grouping_field=[
            AWKClean.establishment_id,
            AWKClean.ascwds_worker_import_date,
            AWKClean.main_job_role_clean_labelled,
        ],
        alias="ascwds_num_of_jobs",
    )
    # worker_record_per_location_count_df.show()

    master_df = master_df.join(
        worker_record_per_location_count_df,
        [
            AWKClean.establishment_id,
            AWKClean.main_job_role_clean_labelled,
            AWKClean.ascwds_worker_import_date,
        ],
        "left",
    )
    # master_df.show()

    master_df = master_df.na.fill(value=0, subset=["ascwds_num_of_jobs"])
    # master_df.show()
    master_df = calculate_job_count_breakdown_by_service(master_df)
    # master_df.show()

    master_df = calculate_job_count_breakdown_by_jobrole(master_df)
    # master_df.sort(
    #     IndCQC.location_id,
    #     IndCQC.cqc_location_import_date,
    #     AWKClean.main_job_role_clean_labelled,
    # ).show(100)

    utils.write_to_parquet(
        master_df,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
        "overwrite",
        PartitionKeys,
    )


# old process
def count_grouped_by_field(
    df: DataFrame, grouping_field: str, alias: str = None
) -> DataFrame:
    output_df = df.groupBy(grouping_field).count()

    if alias:
        output_df = output_df.withColumnRenamed("count", alias)

    return output_df


# old process
def get_comprehensive_list_of_job_roles_to_locations(
    worker_df: DataFrame, master_df: DataFrame
) -> DataFrame:
    unique_jobrole_df = get_distinct_list(
        # worker_df, AWKClean.main_job_role_clean_labelled, alias="main_job_role"
        worker_df,
        AWKClean.main_job_role_clean_labelled,
    )
    master_df = master_df.crossJoin(unique_jobrole_df)
    return master_df


# old process
def get_distinct_list(
    input_df: DataFrame, column_name: str, alias: str = None
) -> DataFrame:
    output_df = input_df.select(column_name).distinct()

    if alias:
        output_df = output_df.withColumnRenamed(column_name, alias)

    return output_df


# old process
def determine_worker_record_to_jobs_ratio(master_df: DataFrame) -> DataFrame:
    master_df = master_df.withColumn(
        "location_jobs_to_model",
        F.greatest(
            F.lit(0),
            F.col(IndCQC.estimate_filled_posts) - F.col("location_worker_records"),
        ),
    )

    return master_df


# old process
def calculate_job_count_breakdown_by_service(master_df: DataFrame) -> DataFrame:
    job_role_breakdown_by_service = (
        master_df.select(
            IndCQC.primary_service_type,
            AWKClean.main_job_role_clean_labelled,
            "ascwds_num_of_jobs",
            IndCQC.cqc_location_import_date,
        )
        .groupBy(
            IndCQC.primary_service_type,
            AWKClean.main_job_role_clean_labelled,
            IndCQC.cqc_location_import_date,
        )
        .agg(F.sum("ascwds_num_of_jobs").alias("ascwds_num_of_jobs_in_service"))
    )
    # job_role_breakdown_by_service.show()
    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "all_ascwds_jobs_in_service",
        F.sum("ascwds_num_of_jobs_in_service").over(
            Window.partitionBy(
                IndCQC.primary_service_type, IndCQC.cqc_location_import_date
            )
        ),
    )
    # job_role_breakdown_by_service.show()
    job_role_breakdown_by_service = job_role_breakdown_by_service.withColumn(
        "estimated_job_role_percentage",
        F.col("ascwds_num_of_jobs_in_service") / F.col("all_ascwds_jobs_in_service"),
    ).drop("ascwds_num_of_jobs_in_service", "all_ascwds_jobs_in_service")
    # job_role_breakdown_by_service.show()

    master_df = master_df.join(
        job_role_breakdown_by_service,
        [
            IndCQC.primary_service_type,
            AWKClean.main_job_role_clean_labelled,
            IndCQC.cqc_location_import_date,
        ],
        "left",
    )

    return master_df


# old process
def calculate_job_count_breakdown_by_jobrole(master_df: DataFrame) -> DataFrame:
    master_df = master_df.withColumn(
        "estimated_jobs_in_role",
        F.col(IndCQC.estimate_filled_posts) * F.col("estimated_job_role_percentage"),
    ).drop("estimated_job_role_percentage")
    # master_df.show()

    master_df = master_df.withColumn(
        "estimated_minus_ascwds",
        F.greatest(
            F.lit(0), F.col("estimated_jobs_in_role") - F.col("ascwds_num_of_jobs")
        ),
    ).drop("estimated_jobs_in_role")
    # master_df.show()

    master_df = master_df.withColumn(
        "sum_of_estimated_minus_ascwds",
        F.sum("estimated_minus_ascwds").over(Window.partitionBy(IndCQC.location_id)),
    )
    # master_df.show()

    master_df = master_df.withColumn(
        "adjusted_job_role_percentage",
        F.coalesce(
            (F.col("estimated_minus_ascwds") / F.col("sum_of_estimated_minus_ascwds")),
            F.lit(0.0),
        ),
    ).drop("estimated_minus_ascwds", "sum_of_estimated_minus_ascwds")
    # master_df.show()

    master_df = master_df.withColumn(
        "estimated_num_of_jobs",
        F.col("location_jobs_to_model") * F.col("adjusted_job_role_percentage"),
    ).drop("location_jobs_to_model", "adjusted_job_role_percentage")
    # master_df.show()

    master_df = master_df.withColumn(
        "estimate_job_role_count",
        F.col("ascwds_num_of_jobs") + F.col("estimated_num_of_jobs"),
    ).drop("location_worker_records")

    return master_df


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
