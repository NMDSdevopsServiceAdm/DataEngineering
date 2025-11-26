from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import projects._03_independent_cqc.utils.utils.utils as utils
from projects._03_independent_cqc._02_clean.utils.utils import (
    create_column_with_repeated_values_removed,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

POSTS_TO_DEFINE_LARGE_PROVIDER = 50
LARGE_PROVIDER = "large provider"
REPETITION_LIMIT_ALL_PROVIDERS = 365
REPETITION_LIMIT_LARGE_PROVIDER = 185

# Pushing to sync data into branch.


def null_ct_values_after_consecutive_repetition(
    df: DataFrame,
    column_to_clean: str,
) -> DataFrame:
    """
    Nulls Capacity Tracker values at locations within a provider when their provider total value repeats for too long.

    When a provider has the same total value for more than a repetition limit, then all their location values are nulled after
    the limit period, until the provider has a new total value.
    The repetition limit is 12 months, unless the provider as a whole is larger than 50 posts then the limit is 6 months.
    The provider size is determined by posts in their NHS Capacity Tracker submissions.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): A column with repeated values.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    provider_values_col = f"{column_to_clean}_provider_sum"
    provider_values_col_dedup = f"{column_to_clean}_provider_sum_deduplicated"

    df = aggregate_values_to_provider_level(df, column_to_clean)

    df = create_column_with_repeated_values_removed(
        df,
        provider_values_col,
        provider_values_col_dedup,
    )

    df = calculate_days_a_provider_has_been_repeating_values(
        df, provider_values_col_dedup
    )
    df = identify_large_providers(df, provider_values_col)
    df = clean_capacity_tracker_posts_repetition(df, column_to_clean)

    # TODO Add a column with wording to show which rows have been filtered for what reason.

    df = df.drop(
        provider_values_col,
        provider_values_col_dedup,
        IndCQC.days_provider_has_repeated_value,
        IndCQC.provider_size_in_capacity_tracker_group,
    )

    return df


def aggregate_values_to_provider_level(df: DataFrame, col_to_sum: str) -> DataFrame:
    """
    Adds a new column with the provider level sum of a given column.
    The new column will be named col_to_sum suffixed with "_provider_sum".

    Args:
        df (DataFrame): A dataframe with providerid and the column_to_sum.
        col_to_sum (str): A column of values to sum.

    Returns:
        DataFrame: The input DataFrame with a new aggregated column.
    """
    w = Window.partitionBy([IndCQC.provider_id, IndCQC.cqc_location_import_date])
    df = df.withColumn(
        f"{col_to_sum}_provider_sum",
        F.sum(col_to_sum).over(w),
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

    Analysis of Capacity Tracker data showed a providers total posts changed more frequently when
    they had 50+ posts. Around 80% of providers had up to posts.

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
) -> DataFrame:
    """
    Nulls values in column_to_clean when days_since_previous_submission is above the limit for the providers size.

    If the location is at a large provider then the repetition limit is less than locations at small providers.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): The column with repeated values.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    df = df.withColumn(
        column_to_clean,
        F.when(
            (
                (
                    F.col(IndCQC.provider_size_in_capacity_tracker_group)
                    == LARGE_PROVIDER
                )
                & (
                    F.col(IndCQC.days_provider_has_repeated_value)
                    <= REPETITION_LIMIT_LARGE_PROVIDER
                )
            )
            | (
                (F.col(IndCQC.provider_size_in_capacity_tracker_group).isNull())
                & (
                    F.col(IndCQC.days_provider_has_repeated_value)
                    <= REPETITION_LIMIT_ALL_PROVIDERS
                )
            ),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    return df
