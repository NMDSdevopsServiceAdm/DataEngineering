from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


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

    # reduce_dataset_to_earliest_file_per_month

    # calculate_time_registered_for
    # calculate_time_since_dormant

    # remove_dual_registration_cqc_care_homes

    # replace_zero_beds_with_null
    # populate_missing_care_home_number_of_beds

    # calculate_ascwds_filled_posts

    # create_column_with_repeated_values_removed - ascwds_filled_posts
    # create_column_with_repeated_values_removed - pir_people_directly_employed_cleaned

    # calculate_filled_posts_per_bed_ratio - ascwds_filled_posts_dedup

    # create_banded_bed_count_column

    # clean_ascwds_filled_post_outliers

    # forward_fill_latest_known_value - ascwds_filled_posts_dedup_clean

    # forward_fill_latest_known_value - pir_people_directly_employed_dedup

    # calculate_filled_posts_per_bed_ratio - ascwds_filled_posts_dedup_clean

    # calculate_filled_posts_per_bed_ratio - ct_care_home_total_employed

    # clean_capacity_tracker_care_home_outliers
    # clean_capacity_tracker_non_res_outliers

    # calculate_care_home_status_count

    utils.sink_to_parquet(
        locations_lf,
        destination,
        partition_cols=cqc_partition_keys,
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
