import sys

from utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.ind_cqc_filled_posts_utils.count_registered_manager_names.count_registered_manager_names import (
    count_registered_manager_names,
)
from utils.ind_cqc_filled_posts_utils.ascwds_job_role_count.ascwds_job_role_count import (
    count_job_role_per_establishment_as_columns,
    list_of_job_roles,
)

from utils.ind_cqc_filled_posts_utils.merge_ascwds_job_role_count_and_ind_cqc_estimates.merge_ascwds_job_role_count_and_ind_cqc_estimates import (
    merge_dataframes,
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
    IndCQC.registered_manager_names,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
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

    estimated_ind_cqc_filled_posts_df = count_registered_manager_names(
        estimated_ind_cqc_filled_posts_df
    )

    count_job_roles_per_establishment_df = count_job_role_per_establishment_as_columns(
        cleaned_ascwds_worker_df, list_of_job_roles
    )

    master_df = merge_dataframes(
        estimated_ind_cqc_filled_posts_df, count_job_roles_per_establishment_df
    )

    utils.write_to_parquet(
        estimated_ind_cqc_filled_posts_df,
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
