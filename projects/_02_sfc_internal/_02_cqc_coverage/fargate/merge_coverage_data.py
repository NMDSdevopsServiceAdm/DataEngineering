import sys

from polars_utils import utils
from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from projects._02_sfc_internal._02_cqc_coverage.fargate.utils import utils as cov_utils
from projects._02_sfc_internal.utils.utils import add_parents_or_singles_and_subs_column
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns

CLEANED_CQC_LOCATIONS_COLUMNS_TO_IMPORT = [
    CQCLClean.location_id,
    CQCLClean.cqc_location_import_date,
    CQCLClean.name,
    CQCLClean.postal_code,
    CQCLClean.provider_id,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.imputed_registration_date,
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.primary_service_type,
    CQCLClean.regulated_activities_offered,
    CQCLClean.specialisms_offered,
    CQCLClean.specialism_dementia,
    CQCLClean.specialism_learning_disabilities,
    CQCLClean.specialism_mental_health,
    CQCLClean.services_offered,
    CQCLClean.current_ons_import_date,
    CQCLClean.current_cssr,
    CQCLClean.current_icb,
    CQCLClean.current_region,
    CQCLClean.current_rural_urban_ind_11,
]

CLEANED_ASCWDS_WORKPLACE_COLUMNS_TO_IMPORT = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.location_id,
    AWPClean.establishment_id,
    AWPClean.organisation_id,
    AWPClean.total_staff,
    AWPClean.worker_records,
    AWPClean.master_update_date,
    AWPClean.master_update_date_org,
    AWPClean.establishment_created_date,
    AWPClean.nmds_id,
    AWPClean.is_parent,
    AWPClean.parent_permission,
    AWPClean.last_logged_in_date,
    AWPClean.la_permission,
    AWPClean.workplace_last_active_date,
    AWPClean.purge_date,
]

# The ratings dataset's location id column is genuinely camelCase
# ("locationId"), confirmed against _01_cqc_ratings' own output schema.
CQC_RATINGS_COLUMNS_TO_IMPORT = [
    CQCLClean.location_id,
    CQCRatingsColumns.date,
    CQCRatingsColumns.overall_rating,
    CQCRatingsColumns.latest_rating_flag,
    CQCRatingsColumns.current_or_historic,
]

CLEANED_CQC_PROVIDERS_COLUMNS_TO_IMPORT = [
    CQCPClean.provider_id,
    CQCPClean.name,
    CQCPClean.cqc_provider_import_date,
]


def main(
    cleaned_cqc_location_source: str,
    ascwds_workplace_source: str,
    cqc_ratings_source: str,
    cleaned_cqc_providers_source: str,
    merged_coverage_destination: str,
    reduced_coverage_destination: str,
) -> None:
    """Merges ASC-WDS, CQC ratings and CQC provider data into the coverage dataset.

    Args:
        cleaned_cqc_location_source (str): Source s3 directory for cleaned CQC
            locations.
        ascwds_workplace_source (str): Source s3 directory for ASC-WDS workplace
            data.
        cqc_ratings_source (str): Source s3 directory for CQC ratings.
        cleaned_cqc_providers_source (str): Source s3 directory for cleaned CQC
            providers.
        merged_coverage_destination (str): Destination s3 directory for the full
            merged coverage dataset.
        reduced_coverage_destination (str): Destination s3 directory for the
            single-month coverage dataset.
    """
    cqc_location_lf = utils.scan_parquet(
        cleaned_cqc_location_source,
        selected_columns=CLEANED_CQC_LOCATIONS_COLUMNS_TO_IMPORT,
    )
    ascwds_workplace_lf = utils.scan_parquet(
        ascwds_workplace_source,
        selected_columns=CLEANED_ASCWDS_WORKPLACE_COLUMNS_TO_IMPORT,
    )
    cqc_ratings_lf = utils.scan_parquet(
        cqc_ratings_source, selected_columns=CQC_RATINGS_COLUMNS_TO_IMPORT
    )
    cqc_providers_lf = utils.scan_parquet(
        cleaned_cqc_providers_source,
        selected_columns=CLEANED_CQC_PROVIDERS_COLUMNS_TO_IMPORT,
    )

    ascwds_workplace_lf = cov_utils.add_removed_by_purge_date_filter_flag(
        ascwds_workplace_lf
    )

    cqc_location_lf = cqc_location_lf.filter(
        earliest_file_per_month_filter_expr(CQCLClean.cqc_location_import_date)
    )
    ascwds_workplace_lf = cov_utils.deduplicate_ascwds_workplace_data(
        ascwds_workplace_lf
    )

    merged_coverage_lf = cov_utils.join_ascwds_data_into_cqc_location_df(
        cqc_location_lf, ascwds_workplace_lf
    )
    merged_coverage_lf = cov_utils.add_flag_for_in_ascwds(merged_coverage_lf)
    merged_coverage_lf = cov_utils.deduplicate_merged_coverage_data(merged_coverage_lf)
    merged_coverage_lf = add_parents_or_singles_and_subs_column(merged_coverage_lf)
    merged_coverage_lf = cov_utils.join_latest_cqc_rating_into_coverage_df(
        merged_coverage_lf, cqc_ratings_lf
    )
    merged_coverage_lf = cov_utils.add_columns_for_locality_manager_dashboard(
        merged_coverage_lf
    )
    merged_coverage_lf = cov_utils.join_provider_name_into_merged_coverage_df(
        merged_coverage_lf, cqc_providers_lf
    )

    utils.sink_to_parquet(merged_coverage_lf, merged_coverage_destination)

    reduced_coverage_lf = utils.filter_to_maximum_value_in_column(
        merged_coverage_lf, CQCLClean.cqc_location_import_date
    )
    utils.sink_to_parquet(reduced_coverage_lf, reduced_coverage_destination)


if __name__ == "__main__":
    print("Fargate job 'merge_coverage_data' starting...")
    print(f"Job parameters: {sys.argv}")

    args = utils.get_args(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--ascwds_workplace_source",
            "Source s3 directory for ASC-WDS workplace data",
        ),
        ("--cqc_ratings_source", "Source s3 directory for parquet CQC ratings dataset"),
        (
            "--cleaned_cqc_providers_source",
            "Source s3 directory for parquet cleaned CQC providers dataset",
        ),
        (
            "--merged_coverage_destination",
            "Destination s3 directory for full parquet",
        ),
        (
            "--reduced_coverage_destination",
            "Destination s3 directory for single month parquet",
        ),
    )
    main(
        args.cleaned_cqc_location_source,
        args.ascwds_workplace_source,
        args.cqc_ratings_source,
        args.cleaned_cqc_providers_source,
        args.merged_coverage_destination,
        args.reduced_coverage_destination,
    )

    print("Fargate job 'merge_coverage_data' complete")
