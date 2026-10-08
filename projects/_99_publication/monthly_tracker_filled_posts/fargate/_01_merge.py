from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.primary_service_type,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.main_job_group_labelled,
    IndCQC.estimate_filled_posts_by_job_role,
    # Location-level, pre-job-role-split estimate - repeated across every job
    # role row for the same location_id/cqc_location_import_date, added here
    # only so the publication clean job can verify its job-role-summed total
    # against this base estimate.
    IndCQC.estimate_filled_posts,
]

JOB_ROLE_METADATA_ARCHIVE_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.ct_non_res_care_workers_employed_imputed,
    IndCQC.care_home_status_count,
]


def main(
    jr_archive_estimates_source: str,
    jr_archive_metadata_source: str,
    merge_data_destination: str,
    run_number: int | None = None,
) -> None:
    """
    Merges archived job role estimates and metadata for one archived run.

    Args:
        jr_archive_estimates_source (str): s3 directory of the archived job role estimates
        jr_archive_metadata_source (str): s3 directory of the archived job role metadata
        merge_data_destination (str): destination s3 directory for merged data
        run_number (int | None): the archived run to merge. Defaults to None, the latest run
    """
    estimates_run_source, metadata_run_source = merge_utils.resolve_run_sources(
        [jr_archive_estimates_source, jr_archive_metadata_source], run_number
    )
    print(f"Merging archived runs: {estimates_run_source} and {metadata_run_source}")

    jr_estimates_lf = utils.scan_parquet(
        estimates_run_source,
        selected_columns=JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
    )
    metadata_lf = utils.scan_parquet(
        metadata_run_source,
        selected_columns=JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
    )

    jr_estimates_lf = jr_estimates_lf.join(
        metadata_lf, on=IndCQC.id_per_locationid_import_date, how="left"
    )

    utils.sink_to_parquet(
        lazy_df=jr_estimates_lf,
        output_path=merge_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--jr_archive_estimates_source",
            "S3 directory of the archived job role estimates, holding all runs",
        ),
        (
            "--jr_archive_metadata_source",
            "S3 directory of the archived job role metadata, holding all runs",
        ),
        (
            "--merge_data_destination",
            "Destination s3 directory for merged data",
        ),
        (
            "--run_number",
            "Archived run number to merge, or 'latest'",
            False,
            merge_utils.LATEST_RUN,
        ),
    )
    main(
        jr_archive_estimates_source=args.jr_archive_estimates_source,
        jr_archive_metadata_source=args.jr_archive_metadata_source,
        merge_data_destination=args.merge_data_destination,
        run_number=merge_utils.parse_run_number(args.run_number),
    )
