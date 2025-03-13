import sys

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.estimate_filled_posts_by_job_role_utils import utils as JRutils
from utils.estimate_filled_posts_by_job_role_utils.models import interpolation
from utils.ind_cqc_filled_posts_utils import utils as FPutils

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
cleaned_ascwds_worker_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
]
estimated_ind_cqc_filled_posts_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.unix_time,
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
    IndCQC.registered_manager_names,
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
    estimated_ind_cqc_filled_posts_df = utils.read_from_parquet(
        estimated_ind_cqc_filled_posts_source,
        selected_columns=estimated_ind_cqc_filled_posts_columns_to_import,
    )
    cleaned_ascwds_worker_df = utils.read_from_parquet(
        cleaned_ascwds_worker_source,
        selected_columns=cleaned_ascwds_worker_columns_to_import,
    )

    aggregated_job_roles_per_establishment_df = (
        JRutils.aggregate_ascwds_worker_job_roles_per_establishment(
            cleaned_ascwds_worker_df, JRutils.list_of_job_roles
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.merge_dataframes(
        estimated_ind_cqc_filled_posts_df,
        aggregated_job_roles_per_establishment_df,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds(
        estimated_ind_cqc_filled_posts_by_job_role_df
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.sum_job_role_count_split_by_service(
            estimated_ind_cqc_filled_posts_by_job_role_df, JRutils.list_of_job_roles
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.transform_job_role_count_map_to_ratios_map(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_counts,
            IndCQC.ascwds_job_role_ratios,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.transform_job_role_count_map_to_ratios_map(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_counts_by_primary_service,
            IndCQC.ascwds_job_role_ratios_by_primary_service,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = FPutils.merge_columns_in_order(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        [
            IndCQC.ascwds_job_role_ratios,
            IndCQC.ascwds_job_role_ratios_by_primary_service,
        ],
        IndCQC.ascwds_job_role_ratios_merged,
        IndCQC.ascwds_job_role_ratios_merged_source,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.create_estimate_filled_posts_by_job_role_map_column(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.unpack_mapped_column(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        IndCQC.estimate_filled_posts_by_job_role,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.count_registered_manager_names(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        interpolation.model_job_role_ratio_interpolation(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_ratios,
            "straight",
        )
    )

    utils.write_to_parquet(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
        "overwrite",
        PartitionKeys,
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
