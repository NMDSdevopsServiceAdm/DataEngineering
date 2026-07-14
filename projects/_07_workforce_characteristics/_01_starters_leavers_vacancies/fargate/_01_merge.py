import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as mUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

workplace_columns = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
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


def main(
    metadata_source: str,
    job_role_estimates_source: str,
    cleaned_ascwds_workplace_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        metadata_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        merged_data_destination (str): destination for merged output
    """

    metadata_lf = utils.scan_parquet(
        source=metadata_source, selected_columns=metadata_columns
    )
    job_role_estimates_lf = utils.scan_parquet(job_role_estimates_source)
    cleaned_ascwds_workplace_lf = utils.scan_parquet(
        cleaned_ascwds_workplace_source
    ).select(
        *[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        mUtils.slv_cols_selector()
    )

    # TODO: Placeholder only
    # mUtils.convert_ascwds_job_role_columns_to_rows()

    # TODO: Placeholder only
    # mUtils.join_datasets()

    # TODO: Placeholder only
    # mUtils.apply_employment_status_magic_numbers()

    utils.sink_to_parquet(
        lazy_df=cleaned_ascwds_workplace_lf,
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
            "--cleaned_ascwds_workplace_source",
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
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        merged_data_destination=args.merged_data_destination,
    )
