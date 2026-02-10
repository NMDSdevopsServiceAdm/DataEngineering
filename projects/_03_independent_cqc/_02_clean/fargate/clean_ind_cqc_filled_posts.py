from dataclasses import dataclass
import polars as pl

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
from projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value import (
    forward_fill_latest_known_value,
)
from projects._03_independent_cqc._02_clean.fargate.utils.utils import (
    create_column_with_repeated_values_removed,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome, Dormancy

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
average_number_of_beds: str = "avg_beds"


@dataclass
class NumericalValues:
    number_of_days_to_forward_fill = 65  # Note: using 65 as a proxy for 2 months


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_destination: str,
) -> None:
    """
    Cleans merged IND CQC Data for subsequent steps.

    Args:
        merged_ind_cqc_source (str): s3 path to the cleaned cqc location data
        cleaned_ind_cqc_destination (str): s3 path to save the output data
    """
    print("Cleaning merged_ind_cqc dataset...")
    locations_df = utils.scan_parquet(merged_ind_cqc_source)
    print("Merged IND CQC location LazyFrame read in")

    locations_df = cUtils.reduce_dataset_to_earliest_file_per_month(locations_df)

    locations_df = calculate_time_registered_for(locations_df)
    locations_df = calculate_time_since_dormant(locations_df)

    locations_df = remove_dual_registration_cqc_care_homes(locations_df)

    print(f"Exporting as parquet to {cleaned_ind_cqc_destination}")
    utils.sink_to_parquet(
        locations_df,
        cleaned_ind_cqc_destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


def calculate_time_registered_for(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a new column called time_registered which is the number of months the location has been registered with CQC for (rounded up).

    This function adds a new integer column to the given data frame which represents the number of months (rounded up) between the
    imputed registration date and the cqc location import date.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame containing the columns: imputed_registration_date and cqc_location_import_date.

    Returns:
        pl.LazyFrame: A polars lazyFrame with the new time_registered column added.
    """
    lf = lf.with_columns(
        (
            pl.date_ranges(
                pl.col(IndCQC.imputed_registration_date),
                pl.col(IndCQC.cqc_location_import_date),
                interval="1mo",
                closed="right",
            ).list.len()
            + 1
        ).alias(IndCQC.time_registered)
    )

    return lf


def calculate_time_since_dormant(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column to show the number of months since the location was last dormant.

    This function calculates the number of months since the last time a location was marked as dormant.
    It tracks the most recent date when dormancy was marked as "Y" and calculates
    the number of months since that date for each location.

    'time_since_dormant' values before the first instance of dormancy are null.
    If the location has never been dormant then 'time_since_dormant' is null.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame with columns: cqc_location_import_date, dormancy, and location_id.

    Returns:
        pl.LazyFrame: A polars LazyFrame with an additional column 'time_since_dormant'.
    """
    lf = lf.sort([IndCQC.location_id, IndCQC.cqc_location_import_date])
    lf = lf.with_columns(
        pl.when(pl.col(IndCQC.dormancy) == Dormancy.dormant)
        .then(pl.col(IndCQC.cqc_location_import_date))
        .otherwise(None)
        .alias(IndCQC.dormant_date)
    )

    lf = lf.with_columns(
        pl.col(IndCQC.dormant_date)
        .forward_fill(limit=None)
        .over(IndCQC.location_id)
        .alias(IndCQC.last_dormant_date)
    )

    lf = lf.with_columns(
        pl.when(pl.col(IndCQC.last_dormant_date).is_not_null())
        .then(
            pl.when(pl.col(IndCQC.dormancy) == Dormancy.dormant)
            .then(1)
            .otherwise(
                (
                    pl.date_ranges(
                        start=pl.col(IndCQC.last_dormant_date),
                        end=pl.col(IndCQC.cqc_location_import_date),
                        interval="1mo",
                        closed="right",
                    ).list.len()
                    + 1
                ).cast(pl.Int64)
            )
        )
        .otherwise(None)
        .alias(IndCQC.time_since_dormant)
    )

    lf = lf.drop(
        [
            IndCQC.dormant_date,
            IndCQC.last_dormant_date,
        ]
    )

    return lf


def remove_dual_registration_cqc_care_homes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Removes cqc care home locations with dual registration and ensures no loss of ascwds data.

    This function removes one instance of cqc care home locations with dual registration. These
    are identified using cqc_location_import_date, name, postcode, and carehome. Any ASCWDS data in either
    location is shared to the other and then the location with the newer registration date is removed.

    The CQC locations dataset includes instances of 'dual registration', where two providers have evidenced to
    CQC that they are both responsible for managing the regulated activities at a single location.
    In this data, these instances appear as two separate lines, with different Location IDs, but with the same
    names and addresses of services. To understand care provision in England accurately, one of these 'dual registered'
    location pairs should be removed.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame containing cqc location data and ascwds data

    Returns:
        pl.LazyFrame: A polars LazyFrame with dual regestrations deduplicated and ascwds data retained.
    """
    duplicate_columns = [
        IndCQC.cqc_location_import_date,
        IndCQC.name,
        IndCQC.postcode,
        IndCQC.care_home,
    ]
    distinguishing_columns = [IndCQC.imputed_registration_date, IndCQC.location_id]
    lf = copy_ascwds_data_across_duplicate_rows(lf, duplicate_columns)
    lf = deduplicate_care_homes(lf, duplicate_columns, distinguishing_columns)
    return lf


def deduplicate_care_homes(
    lf: pl.LazyFrame, duplicate_columns: list[str], distinguishing_columns: list[str]
) -> pl.LazyFrame:
    """
    Removes cqc locations with dual registration.

    This function removes the more recently registered instance of cqc care home locations with dual registration.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame containing cqc location data and ascwds data.
        duplicate_columns (list[str]): A list of column names to identify duplicates.
        distinguishing_columns (list[str]): A list of the columns which will decide which of the duplicates to keep.

    Returns:
        pl.LazyFrame: A polars LazyFrame with dual regestrations deduplicated.
    """
    lf = lf.sort(duplicate_columns + distinguishing_columns)

    care_home_deduped = lf.filter(
        pl.col(IndCQC.care_home) == CareHome.care_home
    ).unique(subset=duplicate_columns, keep="first")

    not_care_home = lf.filter(pl.col(IndCQC.care_home) == CareHome.not_care_home)

    return pl.concat([care_home_deduped, not_care_home])


def copy_ascwds_data_across_duplicate_rows(
    lf: pl.LazyFrame, duplicate_columns: list
) -> pl.LazyFrame:
    """
    Copies total_staff_bounded and worker_records_bounded across duplicate rows.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame containing cqc location data and ascwds data.
        duplicate_columns (list): A list of column names to identify duplicates.

    Returns:
        pl.LazyFrame: A polars LazyFrame with total_staff_bounded and worker_records_bounded copied across duplicate rows.
    """
    lf = lf.with_columns(
        [
            pl.when(pl.col(IndCQC.care_home) == CareHome.care_home)
            .then(
                pl.coalesce(
                    [
                        pl.col(IndCQC.total_staff_bounded),
                        pl.col(IndCQC.total_staff_bounded)
                        .max()
                        .over(duplicate_columns),
                    ]
                )
            )
            .otherwise(pl.col(IndCQC.total_staff_bounded))
            .alias(IndCQC.total_staff_bounded),
            pl.when(pl.col(IndCQC.care_home) == CareHome.care_home)
            .then(
                pl.coalesce(
                    [
                        pl.col(IndCQC.worker_records_bounded),
                        pl.col(IndCQC.worker_records_bounded)
                        .max()
                        .over(duplicate_columns),
                    ]
                )
            )
            .otherwise(pl.col(IndCQC.worker_records_bounded))
            .alias(IndCQC.worker_records_bounded),
        ]
    )

    return lf


if __name__ == "__main__":
    print("Running Clean IND CQC Filled Posts job")

    args = utils.get_args(
        (
            "--merged_ind_cqc_source",
            "Source s3 directory for merge_ind_cqc_data dataset",
        ),
        (
            "--cleaned_ind_cqc_destination",
            "A destination directory for outputting cleaned_ind_cqc_destination",
        ),
    )

    main(
        merged_ind_cqc_source=args.merged_ind_cqc_source,
        cleaned_ind_cqc_destination=args.cleaned_ind_cqc_destination,
    )

    print("Finished Clean IND CQC Filled Posts job")
