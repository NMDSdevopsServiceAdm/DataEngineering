import sys
from datetime import date

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

from utils import utils, cleaning_utils as CUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
    ArchivePartitionKeys as ArchiveKeys,
)

MONTHLY_ARCHIVE_COLUMNS = [
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
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
    IndCQC.imputed_non_res_people_directly_employed,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_with_dormancy_model,
    IndCQC.imputed_posts_rolling_avg_model,
    IndCQC.imputed_ratio_rolling_avg_model,
    IndCQC.imputed_registration_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.number_of_beds,
    IndCQC.organisation_id,
    IndCQC.people_directly_employed_dedup,
    IndCQC.primary_service_type,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.related_location,
    IndCQC.rolling_average_model_filled_posts_per_bed_ratio,
    IndCQC.rolling_average_model,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
]


def main(
    estimate_ind_cqc_filled_posts_source: str,
    monthly_filled_posts_archive_destination: str,
    annual_filled_posts_archive_destination: str,
):
    print("Archiving independent CQC filled posts...")

    spark = utils.get_spark()

    estimate_filled_posts_df = utils.read_from_parquet(
        estimate_ind_cqc_filled_posts_source,
        MONTHLY_ARCHIVE_COLUMNS,
    )

    monthly_estimates_df = utils.filter_df_to_maximum_value_in_column(
        estimate_filled_posts_df, IndCQC.cqc_location_import_date
    )

    monthly_estimates_df = create_archive_date_partition_columns(
        monthly_estimates_df, IndCQC.cqc_location_import_date
    )

    print(f"Exporting as parquet to {monthly_filled_posts_archive_destination}")

    print("Completed archive independent CQC filled posts")


def create_archive_date_partition_columns(df: DataFrame, date_column: str) -> DataFrame:
    """
    Creates columns for archive day, month, and year based on the given data column.

    Args:
        df(DataFrame): A dataframe with a data column.
        date_column(str): A date type column name to be used to construct the partition columns.

    Returns:
        DataFrame: A dataframe with archive day, month, and year columns added.
    """
    date = df.select(date_column).distinct().collect()[0][0]

    day = add_leading_zero(date.day)
    month = add_leading_zero(date.month)
    year = str(date.year)
    df = df.withColumn(ArchiveKeys.archive_day, F.lit(day))
    df = df.withColumn(ArchiveKeys.archive_month, F.lit(month))
    df = df.withColumn(ArchiveKeys.archive_year, F.lit(year))
    return df


def add_leading_zero(date_as_number: int):
    """
    Adds a leading zero to single digit values.

    Args:
        date_as_number(int): An integer.

    Returns:
        str: The integer as a string of at least two characters.
    """
    leading_zero: str = "0"
    largest_single_digit_integer: int = 9
    if date_as_number > largest_single_digit_integer:
        date_as_string = str(date_as_number)
    else:
        leading_zero + str(date_as_number)
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
