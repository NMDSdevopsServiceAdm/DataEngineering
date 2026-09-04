from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Commented out because the sample archive does not reflect the actual archive.
# JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS = [
#     IndCQC.id_per_locationid_import_date,
#     IndCQC.location_id,
#     IndCQC.cqc_location_import_date,
#     IndCQC.primary_service_type,
#     IndCQC.main_job_role_clean_labelled,
#     IndCQC.main_job_group_labelled,
#     IndCQC.estimate_filled_posts_by_job_role_historically_reallocated,
# ]

# JOB_ROLE_METADATA_ARCHIVE_COLUMNS = [
#     IndCQC.id_per_locationid_import_date,
#     IndCQC.ct_care_home_total_employed_imputed,
#     IndCQC.ct_non_res_care_workers_employed_imputed,
# ]


def main(
    jr_archive_estimates_source: str,
    jr_archive_metadata_source: str,
    jr_archive_geography_source: str,
    merge_data_destination: str,
) -> None:
    """
    Merges archived job role estimates, metadata and geography data.

    Args:
        jr_archive_estimates_source (str): source s3 directory for archived job role estimates data
        jr_archive_metadata_source (str): source s3 directory for archived job role metadata data
        jr_archive_geography_source (str): source s3 directory for archived geography data
        merge_data_destination (str): destination s3 directory for merged data
    """
    jr_estimates_lf = utils.scan_parquet(
        jr_archive_estimates_source,
        # selected_columns=JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
    )
    metadata_lf = utils.scan_parquet(
        jr_archive_metadata_source,
        # selected_columns=JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
    )
    geography_lf = utils.scan_parquet(jr_archive_geography_source)

    # See merge_utils/test_merge_utils for placeholders.

    jr_estimates_lf = jr_estimates_lf.join(
        metadata_lf, on=IndCQC.id_per_locationid_import_date, how="left"
    )

    geography_lf = geography_lf.unique()
    jr_estimates_lf = jr_estimates_lf.join(
        geography_lf, on=IndCQC.id_per_locationid_import_date, how="left"
    )

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
