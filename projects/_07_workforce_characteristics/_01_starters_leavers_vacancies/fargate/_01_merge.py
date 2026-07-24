import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as mUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

workplace_columns = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
    AWPJobRoles.job_role_code,
    AWPJobRoles.employees,
    AWPJobRoles.starters,
    AWPJobRoles.leavers,
    AWPJobRoles.vacancies,
]

metadata_columns = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.brand_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type_second_level,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_registration_date,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.estimate_filled_posts_source,
]

job_role_estimates_columns = [
    IndCQC.id_per_locationid_import_date_job_role,
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.primary_service_type,
    IndCQC.id_per_locationid_import_date,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.estimate_filled_posts_by_job_role_historically_reallocated,
    IndCQC.main_job_group_labelled,
]


def main(
    metadata_source: str,
    job_role_estimates_source: str,
    prepared_slv_dataset_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        metadata_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        prepared_slv_dataset_source (str): path to the cleaned ascwds workplace data
        merged_data_destination (str): destination for merged output
    """

    metadata_lf = utils.scan_parquet(
        source=metadata_source, selected_columns=metadata_columns
    )
    job_role_estimates_lf = utils.scan_parquet(
        source=job_role_estimates_source, selected_columns=job_role_estimates_columns
    )
    cleaned_ascwds_workplace_lf = utils.scan_parquet(
        prepared_slv_dataset_source, selected_columns=workplace_columns
    )

    # TODO: Placeholder only
    # mUtils.join_datasets()

    # TODO: Placeholder only
    # mUtils.apply_employment_status_magic_numbers()

    utils.sink_to_parquet(
        lazy_df=job_role_estimates_lf,
        output_path=merged_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--metadata_source",
            "Source s3 directory for metadata",
        ),
        (
            "--job_role_estimates_source",
            "Source s3 directory for job role estimates data",
        ),
        (
            "--prepared_slv_dataset_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        metadata_source=args.metadata_source,
        job_role_estimates_source=args.job_role_estimates_source,
        prepared_slv_dataset_source=args.prepared_slv_dataset_source,
        merged_data_destination=args.merged_data_destination,
    )
