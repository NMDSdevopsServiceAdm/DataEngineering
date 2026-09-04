from polars_utils import utils
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
    IndCQC.location_id,
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
    job_role_geography_source: str,
    job_role_estimates_destination: str,
    job_role_metadata_destination: str,
    job_role_geography_destination: str,
) -> None:
    """
    Archives the independent CQC filled posts by job role estimates, split into three
    column-scoped outputs: estimates, metadata, and geography.

    Args:
        job_role_estimates_source (str): source s3 directory for the job role
            filled posts estimates
        job_role_metadata_source (str): source s3 directory for the job role merge
            metadata
        job_role_geography_source (str): source s3 directory for the independent
            CQC filled posts estimates, used to source location_id and its
            geography columns
        job_role_estimates_destination (str): s3 URI to write the job role estimates
            archive to
        job_role_metadata_destination (str): s3 URI to write the job role metadata
            archive to
        job_role_geography_destination (str): s3 URI to write the job role geography
            archive to
    """
    print("Archiving independent CQC filled posts by job role...")

    job_role_estimates_lf = utils.scan_parquet(
        job_role_estimates_source,
        selected_columns=JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
    )
    job_role_metadata_lf = utils.scan_parquet(
        job_role_metadata_source,
        selected_columns=JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
    )
    job_role_geography_lf = utils.scan_parquet(
        job_role_geography_source,
        selected_columns=JOB_ROLE_GEOGRAPHY_ARCHIVE_COLUMNS,
    )
    # Geography values are fanned out per location earlier in the pipeline, so a
    # plain .unique() safely collapses each location to a single record.
    job_role_geography_lf = job_role_geography_lf.unique()

    print(f"Exporting as parquet to {job_role_estimates_destination}")
    utils.sink_to_parquet(job_role_estimates_lf, job_role_estimates_destination)

    print(f"Exporting as parquet to {job_role_metadata_destination}")
    utils.sink_to_parquet(job_role_metadata_lf, job_role_metadata_destination)

    print(f"Exporting as parquet to {job_role_geography_destination}")
    utils.sink_to_parquet(job_role_geography_lf, job_role_geography_destination)

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
            "--job_role_geography_source",
            "Source s3 directory for the independent CQC filled posts estimates, "
            "used to source location_id and its geography columns",
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
        job_role_geography_source=args.job_role_geography_source,
        job_role_estimates_destination=args.job_role_estimates_destination,
        job_role_metadata_destination=args.job_role_metadata_destination,
        job_role_geography_destination=args.job_role_geography_destination,
    )

    print("Finished Archive Independent CQC Job Role Estimates job")
