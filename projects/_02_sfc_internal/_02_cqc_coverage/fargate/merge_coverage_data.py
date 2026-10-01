import sys

from polars_utils import utils
from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from projects._02_sfc_internal._02_cqc_coverage.fargate.utils import utils as cov_utils
from projects._02_sfc_internal.utils.utils import add_parents_or_singles_and_subs_column
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


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
    cqc_location_lf = utils.scan_parquet(cleaned_cqc_location_source)
    ascwds_workplace_lf = utils.scan_parquet(ascwds_workplace_source)
    cqc_ratings_lf = utils.scan_parquet(cqc_ratings_source)
    cqc_providers_lf = utils.scan_parquet(cleaned_cqc_providers_source)

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
