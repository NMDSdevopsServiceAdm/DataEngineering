from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
)


def clean_people_directly_employed_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of the'pir_people_directly_employed' column.

    This function step duplicates 'pir_people_directly_employed' as 'pir_people_directly_employed_cleaned'
    and then runs various functions designed to remove outliers.

    Args:
        df (DataFrame): A dataframe containing 'pir_people_directly_employed'.

    Returns:
        DataFrame: A dataframe containing 'pir_people_directly_employed_cleaned'.
    """
    print("Cleaning pir_people_directly_employed...")

    df = df.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.col(PIRCols.pir_people_directly_employed),
    )

    df = null_large_single_submission_locations(df)

    # TODO remove locations with large differences (top 1% of (max-min)/avg?)

    return df


def null_large_single_submission_locations(df: DataFrame) -> DataFrame:
    """
    Excludes CQC PIR data for locations who submitted a large employee figure and only submitted once.

    Large locations can have a big impact in localised data. In order to help identify data quality
    issues, we only want to keep data for larger locations who have submitted more than once. This
    is so we have multiple submissions to verify that the location are consistently large as opposed
    to a data input error.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with submission count.
    """
    w = Window.partitionBy(PIRCleanCols.location_id)

    two_submissions: int = 2
    submission_count: str = "submission_count"
    max_people_employed: str = "max_value"
    large_location_identifier: int = 100

    df = df.withColumn(
        submission_count,
        F.count(PIRCleanCols.pir_people_directly_employed_cleaned).over(w),
    )
    df = df.withColumn(
        max_people_employed,
        F.max(PIRCleanCols.pir_people_directly_employed_cleaned).over(w),
    )

    df = df.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.when(
            (F.col(max_people_employed) >= large_location_identifier)
            & (F.col(submission_count) < two_submissions),
            F.lit(None),
        ).otherwise(F.col(PIRCleanCols.pir_people_directly_employed_cleaned)),
    )
    df = df.drop(max_people_employed, submission_count)

    return df
