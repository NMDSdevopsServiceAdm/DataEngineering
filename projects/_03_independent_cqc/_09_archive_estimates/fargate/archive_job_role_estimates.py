from datetime import datetime

import polars as pl

import projects._03_independent_cqc._09_archive_estimates.fargate.utils.archive_utils as aUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.estimate_filled_posts,
    IndCQC.primary_service_type,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.ascwds_job_role_ratios,
    IndCQC.imputed_ascwds_job_role_ratios,
    IndCQC.ascwds_job_role_rolling_ratio,
    IndCQC.ascwds_job_role_ratios_merged,
    IndCQC.ascwds_job_role_ratios_merged_source,
    IndCQC.estimate_filled_posts_by_job_role_manager_adjusted,
    IndCQC.estimate_filled_posts_by_job_role_historically_reallocated,
    IndCQC.main_job_group_labelled,
    IndCQC.job_role_filtering_rule,
]

JOB_ROLE_METADATA_ARCHIVE_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.imputed_registration_date,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.estimate_filled_posts_source,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.care_home_model,
    IndCQC.imputed_pir_filled_posts_model,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_combined_model,
    IndCQC.non_res_combined_model,
    IndCQC.pir_people_directly_employed_dedup,
    IndCQC.posts_rolling_average_model,
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.ct_non_res_care_workers_employed_imputed,
]

JOB_ROLE_GEOGRAPHY_ARCHIVE_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
]


def main(
    job_role_estimates_source: str,
    job_role_metadata_source: str,
    job_role_estimates_destination: str,
    job_role_metadata_destination: str,
    job_role_geography_destination: str,
) -> None:
    """
    Archives the independent CQC filled posts by job role estimates, split into three
    column-scoped outputs: estimates, metadata, and geography.

    Each output is partitioned by archive_date and run_number.
    archive_date is a string formatted as yyyy-mm-dd.
    run_number is an integer that is 1 + current run_number in s3.
    An error is raised if the destinations disagree on the existing run_number.

    Args:
        job_role_estimates_source (str): source s3 directory for the job role
            filled posts estimates
        job_role_metadata_source (str): source s3 directory for the job role merge
            metadata
        job_role_estimates_destination (str): s3 URI to write the job role estimates
            archive to
        job_role_metadata_destination (str): s3 URI to write the job role metadata
            archive to
        job_role_geography_destination (str): s3 URI to write the job role geography
            archive to
    """
    print("Archiving independent CQC filled posts by job role...")

    archive_date = datetime.now().strftime("%Y-%m-%d")
    run_number = (
        aUtils.get_run_number(
            [
                job_role_estimates_destination,
                job_role_metadata_destination,
                job_role_geography_destination,
            ]
        )
        + 1
    )
    partition_keys = [ArchiveKeys.archive_date, ArchiveKeys.run_number]

    job_role_estimates_lf = utils.scan_parquet(
        job_role_estimates_source,
        selected_columns=JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
    )
    job_role_metadata_lf = utils.scan_parquet(
        job_role_metadata_source,
        selected_columns=JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
    )
    job_role_geography_lf = utils.scan_parquet(
        job_role_metadata_source,
        selected_columns=JOB_ROLE_GEOGRAPHY_ARCHIVE_COLUMNS,
    )

    job_role_estimates_lf = job_role_estimates_lf.with_columns(
        pl.lit(archive_date).alias(ArchiveKeys.archive_date),
        pl.lit(run_number).alias(ArchiveKeys.run_number),
    )
    job_role_metadata_lf = job_role_metadata_lf.with_columns(
        pl.lit(archive_date).alias(ArchiveKeys.archive_date),
        pl.lit(run_number).alias(ArchiveKeys.run_number),
    )
    job_role_geography_lf = job_role_geography_lf.with_columns(
        pl.lit(archive_date).alias(ArchiveKeys.archive_date),
        pl.lit(run_number).alias(ArchiveKeys.run_number),
    )

    print(f"Exporting as parquet to {job_role_estimates_destination}")
    utils.sink_to_parquet(
        job_role_estimates_lf,
        job_role_estimates_destination,
        partition_cols=partition_keys,
    )

    print(f"Exporting as parquet to {job_role_metadata_destination}")
    utils.sink_to_parquet(
        job_role_metadata_lf,
        job_role_metadata_destination,
        partition_cols=partition_keys,
    )

    print(f"Exporting as parquet to {job_role_geography_destination}")
    utils.sink_to_parquet(
        job_role_geography_lf,
        job_role_geography_destination,
        partition_cols=partition_keys,
    )

    print("Completed archive independent CQC filled posts by job role")


if __name__ == "__main__":
    print("Running Archive Independent CQC Job Role Estimates job")

    args = utils.get_args(
        (
            "--job_role_estimates_source",
            "Source s3 directory for the job role filled posts estimates",
        ),
        (
            "--job_role_metadata_source",
            "Source s3 directory for the job role merge metadata",
        ),
        (
            "--job_role_estimates_destination",
            "S3 URI to write the job role estimates archive to",
        ),
        (
            "--job_role_metadata_destination",
            "S3 URI to write the job role metadata archive to",
        ),
        (
            "--job_role_geography_destination",
            "S3 URI to write the job role geography archive to",
        ),
    )

    main(
        job_role_estimates_source=args.job_role_estimates_source,
        job_role_metadata_source=args.job_role_metadata_source,
        job_role_estimates_destination=args.job_role_estimates_destination,
        job_role_metadata_destination=args.job_role_metadata_destination,
        job_role_geography_destination=args.job_role_geography_destination,
    )

    print("Finished Archive Independent CQC Job Role Estimates job")
