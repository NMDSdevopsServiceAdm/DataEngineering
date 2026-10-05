from datetime import date, datetime

import polars as pl

import projects._03_independent_cqc._01_filled_posts._07_archive.fargate.utils.archive_utils as aUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLogCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Test commit to change latest commit sha.

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
    IndCQC.estimate_filled_posts_by_job_role_pre_reallocation,
    IndCQC.estimate_filled_posts_by_job_role,
    IndCQC.main_job_group_labelled,
    IndCQC.job_role_filtering_rule,
]

JOB_ROLE_METADATA_ARCHIVE_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
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
    IndCQC.care_home_status_count,
]

MAX_IMPORT_DATE_SOURCE_COLUMNS = {
    RunLogCols.max_ascwds_workplace_import_date: IndCQC.ascwds_workplace_import_date,
    RunLogCols.max_cqc_location_import_date: IndCQC.cqc_location_import_date,
    RunLogCols.max_cqc_pir_import_date: IndCQC.cqc_pir_import_date,
    RunLogCols.max_ct_care_home_import_date: IndCQC.ct_care_home_import_date,
    RunLogCols.max_ct_non_res_import_date: IndCQC.ct_non_res_import_date,
    RunLogCols.max_current_ons_import_date: IndCQC.current_ons_import_date,
}

RUN_LOG_SCHEMA = {
    ArchiveKeys.archive_date: pl.String,
    ArchiveKeys.run_number: pl.Int64,
    RunLogCols.archive_date_time: pl.Datetime,
    **{column: pl.Date for column in MAX_IMPORT_DATE_SOURCE_COLUMNS},
    RunLogCols.approved: pl.Boolean,
    RunLogCols.commit_sha: pl.String,
    RunLogCols.tag: pl.String,
    RunLogCols.selected_for_publication: pl.Boolean,
    RunLogCols.reconciled: pl.Boolean,
    RunLogCols.locally_checked: pl.Boolean,
}


def save_run_log(
    archive_date: str,
    run_number: int,
    archive_date_time: datetime,
    commit_sha: str,
    max_import_dates: dict[str, date | None],
    destination: str,
) -> None:
    """
    Saves a one-row log of this archive run to S3, partitioned by archive_date and
    run_number.

    The approval and check flags are False; tag is null, for later steps to fill in.

    Args:
        archive_date (str): archive date formatted as yyyy-mm-dd
        run_number (int): run number of this archive
        archive_date_time (datetime): when the archive ran
        commit_sha (str): commit sha of the deployed code
        max_import_dates (dict[str, date | None]): latest import date of each
            source dataset, keyed by run log column name
        destination (str): s3 URI to write the run log to
    """
    run_log_lf = pl.LazyFrame(
        {
            ArchiveKeys.archive_date: [archive_date],
            ArchiveKeys.run_number: [run_number],
            RunLogCols.archive_date_time: [archive_date_time],
            **{column: [value] for column, value in max_import_dates.items()},
            RunLogCols.approved: [False],
            RunLogCols.commit_sha: [commit_sha],
            RunLogCols.tag: [None],
            RunLogCols.selected_for_publication: [False],
            RunLogCols.reconciled: [False],
            RunLogCols.locally_checked: [False],
        },
        schema=RUN_LOG_SCHEMA,
    ).select(list(RUN_LOG_SCHEMA))

    print(f"Exporting run log as parquet to {destination}")
    utils.sink_to_parquet(
        run_log_lf,
        destination,
        partition_cols=[ArchiveKeys.archive_date, ArchiveKeys.run_number],
    )


def main(
    job_role_estimates_source: str,
    job_role_metadata_source: str,
    job_role_estimates_destination: str,
    job_role_metadata_destination: str,
    filled_posts_estimates_source: str,
    run_log_destination: str,
    commit_sha: str,
) -> None:
    """
    Archives the independent CQC filled posts by job role estimates, split into two
    column-scoped outputs: estimates and metadata.

    A run log row is also saved. It is excluded from the run_number check so a
    missing log row can't block the next run.

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
        filled_posts_estimates_source (str): source s3 directory for the filled posts
            estimates, used for the run log's latest import dates
        run_log_destination (str): s3 URI to write the run log to
        commit_sha (str): commit sha of the deployed code, recorded in the run log
    """
    print("Archiving independent CQC filled posts by job role...")

    archive_date_time = datetime.now()
    archive_date = archive_date_time.strftime("%Y-%m-%d")
    run_number = (
        aUtils.get_run_number(
            [
                job_role_estimates_destination,
                job_role_metadata_destination,
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

    max_import_dates = (
        utils.scan_parquet(
            filled_posts_estimates_source,
            selected_columns=list(MAX_IMPORT_DATE_SOURCE_COLUMNS.values()),
        )
        .select(
            pl.col(source).max().alias(column)
            for column, source in MAX_IMPORT_DATE_SOURCE_COLUMNS.items()
        )
        .collect()
        .row(0, named=True)
    )

    job_role_estimates_lf = job_role_estimates_lf.with_columns(
        pl.lit(archive_date).alias(ArchiveKeys.archive_date),
        pl.lit(run_number).alias(ArchiveKeys.run_number),
    )
    job_role_metadata_lf = job_role_metadata_lf.with_columns(
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

    save_run_log(
        archive_date,
        run_number,
        archive_date_time,
        commit_sha,
        max_import_dates,
        run_log_destination,
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
            "--filled_posts_estimates_source",
            "Source s3 directory for the filled posts estimates",
        ),
        ("--run_log_destination", "S3 URI to write the run log to"),
        ("--commit_sha", "Commit sha of the deployed code"),
    )

    main(
        job_role_estimates_source=args.job_role_estimates_source,
        job_role_metadata_source=args.job_role_metadata_source,
        job_role_estimates_destination=args.job_role_estimates_destination,
        job_role_metadata_destination=args.job_role_metadata_destination,
        filled_posts_estimates_source=args.filled_posts_estimates_source,
        run_log_destination=args.run_log_destination,
        commit_sha=args.commit_sha,
    )

    print("Finished Archive Independent CQC Job Role Estimates job")
