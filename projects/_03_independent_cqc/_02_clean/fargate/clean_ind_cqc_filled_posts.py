import polars as pl

import polars_utils.cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator import (
    calculate_ascwds_filled_posts,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers import (
    clean_ascwds_filled_post_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_grouped_providers import (
    GROUPED_PROVIDER_SCHEMA,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_care_home_outliers import (
    clean_capacity_tracker_care_home_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_non_res_outliers import (
    clean_capacity_tracker_non_res_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ind_cqc_filled_posts_utils import (
    calculate_care_home_status_count,
    calculate_time_registered_for,
    calculate_time_since_dormant,
    populate_missing_care_home_number_of_beds,
    remove_dual_registration_cqc_care_homes,
    replace_zero_beds_with_null,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_destination: str,
    grouped_providers_destination: str,
) -> None:
    """
    Cleans independent CQC locations data.

    Args:
        merged_ind_cqc_source (str): s3 path to the merged independent CQC location data
        cleaned_ind_cqc_destination (str): s3 path to save cleaned independent CQC location data
        grouped_providers_destination (str): S3 path to save potential grouped providers data
    """
    print("Cleaning merged_ind_cqc dataset...")

    locations_lf = utils.scan_parquet(merged_ind_cqc_source)
    print("Merged independent CQC location LazyFrame read in")

    locations_lf = locations_lf.filter(earliest_file_per_month_filter_expr())

    locations_lf = calculate_time_registered_for(locations_lf)
    locations_lf = calculate_time_since_dormant(locations_lf)

    locations_lf = remove_dual_registration_cqc_care_homes(locations_lf)

    locations_lf = replace_zero_beds_with_null(locations_lf)
    locations_lf = populate_missing_care_home_number_of_beds(locations_lf)

    locations_lf = calculate_ascwds_filled_posts(locations_lf)

    locations_lf = cUtils.remove_repeated_values_over_time(
        locations_lf,
        columns_to_clean=[
            IndCQC.ascwds_filled_posts,
            IndCQC.pir_people_directly_employed_cleaned,
        ],
        partition_by_columns=IndCQC.location_id,
        date_column=IndCQC.cqc_location_import_date,
    )

    locations_lf = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_lf,
        IndCQC.ascwds_filled_posts_dedup,
        IndCQC.filled_posts_per_bed_ratio,
    )

    locations_lf = cUtils.create_banded_bed_count_column(
        locations_lf,
        IndCQC.number_of_beds_banded,
        [0, 1, 3, 5, 10, 15, 20, 25, 50, float("Inf")],
    )

    try:
        grouped_providers_lf = utils.scan_parquet(
            source=grouped_providers_destination, schema=GROUPED_PROVIDER_SCHEMA
        )
        print("Existing grouped providers read in")
    except FileNotFoundError:
        grouped_providers_lf = pl.LazyFrame(schema=GROUPED_PROVIDER_SCHEMA)
        print("No existing grouped providers found, starting fresh")

    locations_lf, grouped_providers_lf = clean_ascwds_filled_post_outliers(
        locations_lf, grouped_providers_lf
    )

    locations_lf = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_lf,
        IndCQC.ct_care_home_total_employed,
        IndCQC.ct_care_home_posts_per_bed_ratio,
    )

    locations_lf = clean_capacity_tracker_care_home_outliers(locations_lf)
    locations_lf = clean_capacity_tracker_non_res_outliers(locations_lf)

    locations_lf = calculate_care_home_status_count(locations_lf)

    locations_lf = locations_lf.with_columns(
        pl.col(IndCQC.ascwds_filled_posts_source).cast(
            CatColType.AscwdsFilledPostsSourceEnumType
        ),
        pl.col(IndCQC.ascwds_filtering_rule).cast(
            CatColType.AscwdsFilteringRuleEnumType
        ),
    )

    print(f"Exporting cleaned data to {cleaned_ind_cqc_destination}")
    print(f"Exporting grouped providers data to {grouped_providers_destination}")

    utils.sink_to_parquet(
        locations_lf,
        cleaned_ind_cqc_destination,
    )

    utils.sink_to_parquet(
        grouped_providers_lf,
        grouped_providers_destination,
    )


if __name__ == "__main__":
    print("Running Clean Independent CQC job")

    args = utils.get_args(
        (
            "--merged_ind_cqc_source",
            "S3 URI to read merged CQC location data from",
        ),
        (
            "--cleaned_ind_cqc_destination",
            "S3 URI to save cleaned ind cqc data to",
        ),
        (
            "--grouped_providers_destination",
            "S3 URI to save potential grouped providers data to",
        ),
    )

    main(
        merged_ind_cqc_source=args.merged_ind_cqc_source,
        cleaned_ind_cqc_destination=args.cleaned_ind_cqc_destination,
        grouped_providers_destination=args.grouped_providers_destination,
    )

    print("Finished Clean Independent CQC job")
