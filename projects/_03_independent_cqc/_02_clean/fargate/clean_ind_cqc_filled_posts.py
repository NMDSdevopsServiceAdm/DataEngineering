from polars_utils import utils
import polars_utils.cleaning_utils as cUtils
from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator import (
    calculate_ascwds_filled_posts,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers import (
    clean_ascwds_filled_post_outliers,
)

from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_care_home_outliers import (
    clean_capacity_tracker_care_home_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_non_res_outliers import (
    clean_capacity_tracker_non_res_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ind_cqc_filled_posts_utils import (
    remove_dual_registration_cqc_care_homes,
    replace_zero_beds_with_null,
    populate_missing_care_home_number_of_beds,
    calculate_time_registered_for,
    calculate_time_since_dormant,
    calculate_care_home_status_count,
)

from projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value import (
    forward_fill_latest_known_value,
)
from projects._03_independent_cqc._02_clean.fargate.utils.utils import (
    create_column_with_repeated_values_removed,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def main(
    merged_ind_cqc_source: str,
    destination: str,
) -> None:
    """
    Cleans independent CQC locations data.

    Args:
        merged_ind_cqc_source (str): s3 path to the merged independent CQC location data
        destination (str): s3 path to save cleaned independent CQC location data
    """
    print("Cleaning merged_ind_cqc dataset...")

    locations_lf = utils.scan_parquet(merged_ind_cqc_source)
    print("Merged independent CQC location LazyFrame read in")

    locations_lf = cUtils.reduce_dataset_to_earliest_file_per_month(locations_lf)

    locations_lf = calculate_time_registered_for(locations_lf)
    locations_lf = calculate_time_since_dormant(locations_lf)

    locations_lf = remove_dual_registration_cqc_care_homes(locations_lf)

    locations_lf = replace_zero_beds_with_null(locations_lf)
    locations_lf = populate_missing_care_home_number_of_beds(locations_lf)

    locations_lf = calculate_ascwds_filled_posts(
        locations_lf,
        IndCQC.total_staff_bounded,
        IndCQC.worker_records_bounded,
        IndCQC.ascwds_filled_posts,
        IndCQC.ascwds_filled_posts_source,
    )

    locations_lf = create_column_with_repeated_values_removed(
        locations_lf,
        IndCQC.ascwds_filled_posts,
        IndCQC.ascwds_filled_posts_dedup,
    )
    locations_lf = create_column_with_repeated_values_removed(
        locations_lf,
        IndCQC.pir_people_directly_employed_cleaned,
        IndCQC.pir_people_directly_employed_dedup,
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

    locations_lf = clean_ascwds_filled_post_outliers(locations_lf)

    locations_lf = forward_fill_latest_known_value(
        locations_lf, IndCQC.ascwds_filled_posts_dedup_clean
    )

    locations_lf = forward_fill_latest_known_value(
        locations_lf, IndCQC.pir_people_directly_employed_dedup
    )

    locations_lf = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_lf,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.filled_posts_per_bed_ratio,
    )

    locations_lf = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_lf,
        IndCQC.ct_care_home_total_employed,
        IndCQC.ct_care_home_posts_per_bed_ratio,
    )

    locations_lf = clean_capacity_tracker_care_home_outliers(locations_lf)
    # locations_lf = clean_capacity_tracker_non_res_outliers(locations_lf)

    locations_lf = calculate_care_home_status_count(locations_lf)

    print(f"Exporting as parquet to {destination}")

    utils.sink_to_parquet(
        locations_lf,
        destination,
        append=False,
    )


if __name__ == "__main__":
    print("Running Clean Independent CQC job")

    args = utils.get_args(
        (
            "--merged_ind_cqc_source",
            "S3 URI to read merged CQC location data from",
        ),
        (
            "--destination",
            "S3 URI to save cleaned ind cqc data to",
        ),
    )

    main(
        merged_ind_cqc_source=args.merged_ind_cqc_source,
        destination=args.destination,
    )

    print("Finished Clean Independent CQC job")
