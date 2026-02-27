from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as TempCol,
)
from utils.column_values.categorical_column_values import CareHome


def clean_non_residential_rate_of_change(df: DataFrame) -> DataFrame:
    """
    Cleans the rate of change values for non-residential locations by applying
    percentile-based thresholds.

    For non-residential rows, the function calculates the absolute and
    percentage change between the current and previous period values. It then
    computes upper thresholds for both absolute and percentage change based on
    specified percentiles of the distribution of changes in non-residential
    rows. A lower threshold for percentage change is also calculated as the
    reciprocal of the upper percentage change threshold.

    Args:
        df (DataFrame): The input DataFrame containing the current and previous values.

    Returns:
        DataFrame: The DataFrame with cleaned current and previous period columns.
    """
    prev_col = TempCol.previous_period_interpolated
    curr_col = TempCol.current_period_interpolated

    df = calculate_absolute_and_percentage_change(df, prev_col, curr_col)

    abs_upper, perc_upper, perc_lower = compute_non_residential_thresholds(
        df, prev_col, curr_col
    )

    keep_condition = build_non_residential_keep_condition(
        prev_col, curr_col, abs_upper, perc_upper, perc_lower
    )

    return apply_rate_of_change_cleaning(df, prev_col, curr_col, keep_condition)


def calculate_absolute_and_percentage_change(
    df: DataFrame, prev_col: str, curr_col: str
) -> DataFrame:
    """
    Calculates the absolute and percentage rate of change columns.

    Args:
        df (DataFrame): The input DataFrame.
        prev_col (str): Column containing previous period values.
        curr_col (str): Column containing current period values.

    Returns:
        DataFrame: The DataFrame with absolute change and percentage change.
    """
    return df.withColumns(
        {
            TempCol.abs_change: F.abs(F.col(curr_col) - F.col(prev_col)),
            TempCol.perc_change: F.try_divide(F.col(curr_col) / F.col(prev_col)),
        },
    )


def compute_non_residential_thresholds(
    df: DataFrame,
    prev_col: str,
    curr_col: str,
    abs_percentile: float = 0.99,
    perc_percentile: float = 0.99,
) -> tuple[float, float, float]:
    """
    Computes percentile-based filtering thresholds for non-residential rows.

    Rows included are included where:
        - location is not a care home
        - both values non-null
        - at least one value > 10
        - prev != curr.

    At the time of making this function, the approximate thresholds were:
        - abs_upper = ~20 (abs_percentile = 0.99)
        - perc_upper = ~1.24 (perc_percentile = 0.99)

    Args:
        df (DataFrame): The input DataFrame.
        prev_col (str): Column containing previous period values.
        curr_col (str): Column containing current period values.
        abs_percentile (float): Percentile to use for absolute change threshold.
        perc_percentile (float): Percentile to use for percentage change threshold.

    Returns:
        tuple[float, float, float]: Containing abs_upper, perc_upper and
        perc_lower
    """
    df_filtered = df.filter(
        (F.col(IndCqc.care_home) == CareHome.not_care_home)
        & F.col(prev_col).isNotNull()
        & F.col(curr_col).isNotNull()
        & ((F.col(prev_col) > 10) | (F.col(curr_col) > 10))
        & (F.col(prev_col) != F.col(curr_col))
    )

    row = df_filtered.agg(
        F.percentile_approx(TempCol.abs_change, abs_percentile).alias(TempCol.abs_pct),
        F.percentile_approx(TempCol.perc_change, perc_percentile).alias(
            TempCol.perc_pct
        ),
    ).first()

    abs_upper = row[TempCol.abs_pct]
    perc_upper = row[TempCol.perc_pct]
    perc_lower = 1 / perc_upper if perc_upper else None

    return abs_upper, perc_upper, perc_lower


def build_non_residential_keep_condition(
    prev_col: str,
    curr_col: str,
    abs_upper: float,
    perc_upper: float,
    perc_lower: float,
) -> Column:
    """
    Constructs a boolean Column to determine which rows to retain.

    Always keep care homes and non-residential rows where both previous and
    current values are both <= 10 (small changes produce large percentage
    changes). For other rows, only keep if absolute and percentage change are
    within thresholds.

    Args:
        prev_col (str): Column containing previous period values.
        curr_col (str): Column containing current period values.
        abs_upper (float): Absolute change threshold.
        perc_upper (float): Upper percentage change threshold.
        perc_lower (float): Lower percentage change threshold.

    Returns:
        Column: A boolean Column indicating which rows to retain.
    """
    is_care_home = F.col(IndCqc.care_home) == CareHome.care_home
    is_small_non_res = (
        (F.col(IndCqc.care_home) == CareHome.not_care_home)
        & (F.col(prev_col) <= 10)
        & (F.col(curr_col) <= 10)
    )
    is_non_res_within_thresholds = (
        (F.col(IndCqc.care_home) == CareHome.not_care_home)
        & (F.col(TempCol.abs_change) <= F.lit(abs_upper))
        & (F.col(TempCol.perc_change) <= F.lit(perc_upper))
        & (F.col(TempCol.perc_change) >= F.lit(perc_lower))
    )

    return is_care_home | is_small_non_res | is_non_res_within_thresholds


def apply_rate_of_change_cleaning(
    df: DataFrame, prev_col: str, curr_col: str, keep_condition: bool
) -> DataFrame:
    """
    Sets current and previous values to null where keep_condition is not met.

    Creates two new columns with cleaned values which retain original values
    where keep_condition is True, and nulls otherwise.

    Args:
        df (DataFrame): The input DataFrame.
        prev_col (str): Column containing previous period values.
        curr_col (str): Column containing current period values.
        keep_condition (bool): Boolean condition determining which rows
            are retained.

    Returns:
        DataFrame: The DataFrame with cleaned current and previous period columns.
    """
    return df.withColumns(
        {
            TempCol.previous_period_cleaned: F.when(keep_condition, F.col(prev_col)),
            TempCol.current_period_cleaned: F.when(keep_condition, F.col(curr_col)),
        }
    )
