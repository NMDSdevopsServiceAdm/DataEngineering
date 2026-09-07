from polars_utils import utils
from projects._08_publication._01_job_role_estimates.fargate.utils import (
    merge_utils as mUtils,
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


def main(
    jr_archive_estimates_source: str,
    jr_archive_metadata_source: str,
    jr_archive_geography_source: str,
    merge_data_destination: str,
) -> None:
    """
    Merges archived job role estimates, metadata and geography data.

    The geography join is a placeholder.

    Args:
        jr_archive_estimates_source (str): source s3 directory for archived job role estimates data
        jr_archive_metadata_source (str): source s3 directory for archived job role metadata data
        jr_archive_geography_source (str): source s3 directory for archived geography data
        merge_data_destination (str): destination s3 directory for merged data
    """
    jr_estimates_lf = utils.scan_parquet(
        jr_archive_estimates_source,
        selected_columns=JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
    )
    metadata_lf = utils.scan_parquet(
        jr_archive_metadata_source,
        selected_columns=JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
    )
    geography_lf = utils.scan_parquet(jr_archive_geography_source)

    # See merge_utils/test_merge_utils for placeholders.

    jr_estimates_lf = jr_estimates_lf.join(
        metadata_lf, on=IndCQC.id_per_locationid_import_date, how="left"
    )

    # TODO: mUtils.join_geography(merged_lf, geography_lf).

    utils.sink_to_parquet(
        lazy_df=jr_estimates_lf,
        output_path=merge_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--jr_archive_estimates_source",
            "Source s3 directory for archived job role estimates data",
        ),
        (
            "--jr_archive_metadata_source",
            "Source s3 directory for archived job role metadata data",
        ),
        (
            "--jr_archive_geography_source",
            "Source s3 directory for archived geography data",
        ),
        (
            "--merge_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        jr_archive_estimates_source=args.jr_archive_estimates_source,
        jr_archive_metadata_source=args.jr_archive_metadata_source,
        jr_archive_geography_source=args.jr_archive_geography_source,
        merge_data_destination=args.merge_data_destination,
    )
