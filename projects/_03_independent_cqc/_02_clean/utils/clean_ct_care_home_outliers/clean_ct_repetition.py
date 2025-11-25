from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import projects._03_independent_cqc.utils.utils.utils as utils
from projects._03_independent_cqc._02_clean.utils.utils import (
    create_column_with_repeated_values_removed,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

POSTS_TO_DEFINE_LARGE_PROVIDER = 50
LARGE_PROVIDER = "large provider"
REPETITION_LIMIT_ALL_LOCATIONS = 365
REPETITION_LIMIT_LARGE_PROVIDER = 185


def null_ct_values_after_consecutive_repetition(
    df: DataFrame,
    column_to_clean: str,
    add_as_new_column: bool,
) -> DataFrame:
    """
    Adds a new column in which values are allowed to consecutively repeat for a limited time, but are then replaced with null.
    The repetition limit is 12 months, unless the provider as a whole is larger than 50 posts then the limit is 6 months.
    The provider size is determined by posts in their NHS Capacity Tracker submissions.

    This function:
        1. Adds a new column with deduplicated column_to_clean values.
        2. Uses the deduplicated values to calculate the days between import_date and and the date a repeated value began.
        4. Nulls values when the days between dates is above REPETITION_LIMIT.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): A column with repeated values.
        add_as_new_column (bool): True will created a new column with name of column_to_clean suffixed with "_cleaned".

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    provider_level_values = "provider_level_values_column"
    w = Window.partitionBy([IndCQC.provider_id, IndCQC.cqc_location_import_date])
    df = df.withColumn(
        provider_level_values,
        F.sum(column_to_clean).over(w),
    )

    provider_level_values_deduplicated = "provider_level_values_column"
    df = create_column_with_repeated_values_removed(
        df,
        provider_level_values,
        provider_level_values_deduplicated,
    )

    df = calculate_days_a_provider_has_been_repeating_values(
        df, provider_level_values_deduplicated
    )
    df = identify_large_providers(df, provider_level_values)
    df = clean_capacity_tracker_posts_repetition(df, column_to_clean, add_as_new_column)

    df = df.drop(
        provider_level_values,
        provider_level_values_deduplicated,
        IndCQC.days_provider_has_repeated_value,
        IndCQC.provider_size_in_capacity_tracker_group,
    )

    return df


def calculate_days_a_provider_has_been_repeating_values(
    df: DataFrame, provider_level_values_deduplicated: str
) -> DataFrame:
    """
    Adds a column with the number of days between import date and the date a repeated value began.

    Args:
        df (DataFrame): A dataframe with import date and a deduplicated value column.
        provider_level_values_deduplicated (str): A column with repeated values removed.

    Returns:
        DataFrame: The input DataFrame with a new column days_since_previous_submission.
    """
    window_spec = Window.partitionBy(IndCQC.provider_id).orderBy(
        IndCQC.cqc_location_import_date
    )
    window_spec_backwards = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df = utils.get_selected_value(
        df,
        window_spec_backwards,
        provider_level_values_deduplicated,
        IndCQC.cqc_location_import_date,
        IndCQC.previous_submission_import_date,
        "last",
    )

    df = df.withColumn(
        IndCQC.days_provider_has_repeated_value,
        F.date_diff(
            IndCQC.cqc_location_import_date, IndCQC.previous_submission_import_date
        ),
    )

    return df.drop(IndCQC.previous_submission_import_date)


def identify_large_providers(df: DataFrame, provider_level_values: str) -> DataFrame:
    """
    Adds a column to flag large providers.

    The lenght of time that locations have the same value in the Capacity Tracker is much shorter when
    the provider has more than 50 posts.

    Args:
        df (DataFrame): A dataframe with import date and a deduplicated value column.
        provider_level_values (str): The column to sum to determine size.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    df = df.withColumn(
        IndCQC.provider_size_in_capacity_tracker_group,
        F.when(
            F.col(provider_level_values) > POSTS_TO_DEFINE_LARGE_PROVIDER,
            F.lit(LARGE_PROVIDER),
        ).otherwise(None),
    )

    return df.drop(IndCQC.provider_size_in_capacity_tracker)


def clean_capacity_tracker_posts_repetition(
    df: DataFrame,
    column_to_clean: str,
    add_as_new_column: bool,
) -> DataFrame:
    """
    Nulls values in column_to_clean when days_since_previous_submission column is above limit the locations provider size.

    If the location is at a large provider then the repetition limit is less than locations at small providers.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): The column with repeated values.
        add_as_new_column (bool): True will created a new column with name of column_to_clean suffixed with "_cleaned".

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    if add_as_new_column:
        column_to_clean = f"{column_to_clean}_cleaned"

    df = df.withColumn(
        column_to_clean,
        F.when(
            (
                (
                    F.col(IndCQC.provider_size_in_capacity_tracker_group)
                    == LARGE_PROVIDER
                )
                & (
                    F.col(IndCQC.days_since_previous_submission)
                    <= REPETITION_LIMIT_LARGE_PROVIDER
                )
            )
            | (
                (F.col(IndCQC.provider_size_in_capacity_tracker_group).isNull())
                & (
                    F.col(IndCQC.days_since_previous_submission)
                    <= REPETITION_LIMIT_ALL_LOCATIONS
                )
            ),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    return df

    return df
