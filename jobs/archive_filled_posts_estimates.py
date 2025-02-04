import sys
from datetime import datetime, date

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    ArchivePartitionKeys as ArchiveKeys,
)

MONTHLY_ARCHIVE_COLUMNS = [
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filtering_rule,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.care_home,
    IndCQC.care_home_model,
    IndCQC.cqc_location_import_date,
    IndCQC.cqc_pir_import_date,
    IndCQC.current_cssr,
    IndCQC.current_icb,
    IndCQC.current_region,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.dormancy,
    IndCQC.establishment_id,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    IndCQC.imputed_gac_service_types,
    IndCQC.imputed_non_res_pir_people_directly_employed,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_with_dormancy_model,
    IndCQC.imputed_filled_post_model,
    IndCQC.imputed_filled_posts_per_bed_ratio_model,
    IndCQC.imputed_registration_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.number_of_beds,
    IndCQC.organisation_id,
    IndCQC.pir_people_directly_employed_dedup,
    IndCQC.primary_service_type,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.related_location,
    IndCQC.rolling_average_model,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
]
partition_keys = [
    ArchiveKeys.archive_year,
    ArchiveKeys.archive_month,
    ArchiveKeys.archive_day,
    ArchiveKeys.archive_timestamp,
]


def main(
    estimate_ind_cqc_filled_posts_source: str,
    monthly_filled_posts_archive_destination: str,
    annual_filled_posts_archive_destination: str,
):
    print("Archiving independent CQC filled posts...")

    timestamp = datetime.now()

    estimate_filled_posts_df = utils.read_from_parquet(
        estimate_ind_cqc_filled_posts_source,
        MONTHLY_ARCHIVE_COLUMNS,
    )

    monthly_estimates_df = select_import_dates_to_archive(estimate_filled_posts_df)

    monthly_estimates_df = create_archive_date_partition_columns(
        monthly_estimates_df, timestamp
    )

    print(f"Exporting as parquet to {monthly_filled_posts_archive_destination}")

    utils.write_to_parquet(
        monthly_estimates_df,
        monthly_filled_posts_archive_destination,
        mode="append",
        partitionKeys=partition_keys,
    )

    print("Completed archive independent CQC filled posts")


def select_import_dates_to_archive(df: DataFrame) -> DataFrame:
    """
    Filters dataframe to only include the most recent monthly estimates, plus historical annual estimates.

    Args:
        df (DataFrame): A dataframe to archive.

    Returns:
        DataFrame: A dataframe with the most recent monthly estimates, plus historical annual estimates.
    """
    list_of_import_dates = (
        df.select(IndCQC.cqc_location_import_date)
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    most_recent_annual_estimates = identify_date_of_most_recent_annual_estimates(df)

    list_of_import_dates_to_archive = identify_import_dates_to_archive(
        list_of_import_dates, most_recent_annual_estimates
    )

    df = df.where(
        F.col(IndCQC.cqc_location_import_date).isin(list_of_import_dates_to_archive)
    )
    return df


def identify_import_dates_to_archive(
    list_of_import_dates: list, most_recent_annual_estimates: date
) -> list:
    """
    Selects which import dates should be archived from a list of all import dates in the dataset.

    Args:
        list_of_import_dates (list): A list of import dates.
        most_recent_annual_estimates (date): The most recent annual estimates date in the dataframe.

    Returns:
        list: A list of import dates on which to filter the dataset.
    """
    list_of_import_dates_to_archive = []
    for import_date in list_of_import_dates:
        if import_date >= most_recent_annual_estimates:
            list_of_import_dates_to_archive.append(import_date)
        if (import_date.month == most_recent_annual_estimates.month) & (
            import_date < most_recent_annual_estimates
        ):
            list_of_import_dates_to_archive.append(import_date)
    return list_of_import_dates_to_archive


def identify_date_of_most_recent_annual_estimates(df: DataFrame) -> date:
    """
    Identifies the most recent annual estimates date in the dataframe.

    Args:
        df (DataFrame): A dataframe to archive.

    Returns:
        date: The most recent annual estimates date in the dataframe.
    """
    most_recent_import_date = df.agg(F.max(IndCQC.cqc_location_import_date)).collect()[
        0
    ][0]
    march = 3
    april = 4
    first_of_month = 1

    if most_recent_import_date.month <= march:
        previous_year = most_recent_import_date.year - 1
        most_recent_annual_estimates = date(previous_year, april, first_of_month)
    else:
        most_recent_annual_estimates = date(
            most_recent_import_date.year, april, first_of_month
        )
    return most_recent_annual_estimates


def create_archive_date_partition_columns(
    df: DataFrame, date_time: datetime
) -> DataFrame:
    """
    Creates columns for archive day, month, year, timestamp, and run count based on the given datetime.

    Args:
        df(DataFrame): A dataframe with a data column.
        date_time(datetime): A date time to be used to construct the partition columns.

    Returns:
        DataFrame: A dataframe with archive day, month, and year columns added.
    """

    day = add_leading_zero(str(date_time.day))
    month = add_leading_zero(str(date_time.month))
    year = str(date_time.year)
    timestamp = str(date_time)[:16]
    df = df.withColumn(ArchiveKeys.archive_day, F.lit(day))
    df = df.withColumn(ArchiveKeys.archive_month, F.lit(month))
    df = df.withColumn(ArchiveKeys.archive_year, F.lit(year))
    df = df.withColumn(ArchiveKeys.archive_timestamp, F.lit(timestamp))
    return df


def add_leading_zero(date_as_number: int) -> str:
    """
    Adds a leading zero to single digit values.

    Args:
        date_as_number(int): An integer.

    Returns:
        str: The integer as a string of at least two characters.
    """
    leading_zero: str = "0"
    largest_single_digit_integer: int = 9
    if int(date_as_number) > largest_single_digit_integer:
        date_as_string = str(date_as_number)
    else:
        date_as_string = leading_zero + str(date_as_number)
    return date_as_string


if __name__ == "__main__":
    print("Spark job 'archive_filled_posts_estimates' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_ind_cqc_filled_posts_source,
        monthly_filled_posts_archive_destination,
        annual_filled_posts_archive_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated_ind_cqc_filled_posts",
        ),
        (
            "--monthly_filled_posts_archive_destination",
            "Destination s3 directory for monthly filled posts estimates archive",
        ),
        (
            "--annual_filled_posts_archive_destination",
            "Destination s3 directory for annual filled posts estimates archive",
        ),
    )

    main(
        estimate_ind_cqc_filled_posts_source,
        monthly_filled_posts_archive_destination,
        annual_filled_posts_archive_destination,
    )
