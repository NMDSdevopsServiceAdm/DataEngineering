from dataclasses import dataclass

import polars as pl

import polars_utils.cleaning_utils as pUtils
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome


@dataclass
class SetValuesForWinsorization:
    """
    Set numerical values for winsorization process.

    We are identifying the top and bottom 2.5% of entries as outliers (which
    combine to 0.05/5% of data points to remove)

    Following investigation with ASCWDS data alongside Capacity Tracker tracker
    data we were able to validate that the majority of ratios between 0.75 and
    5.0 were closely matched to each other. However, outside of these ratios the
    quality of matches decreased significantly. However, for small care homes
    (lower number of beds) the ratios then to be higher than 0.75 and 5.0 so we
    are using the standardised residual percentile cutoffs if they come out
    higher than these minimum permitted ratios.

    Attributes:
        PERCENTAGE_OF_DATA_TO_REMOVE_AS_OUTLIERS (float): Sets the proportion of
            data to identify as outliers (where 0.05 represents 5%, which would
            identify the top and bottom 2.5% as outliers)
        MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF (float): Sets the lowest minimum
            filled_posts_per_bed_ratio permitted
        MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF (float): Sets the lowest maximum
            filled_posts_per_bed_ratio permitted
    """

    PERCENTAGE_OF_DATA_TO_REMOVE_AS_OUTLIERS: float = 0.05
    MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF: float = 0.75
    MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF: float = 5.0


def winsorize_care_home_filled_posts_per_bed_ratio_outliers(
    input_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Winsorize ASCWDS filled posts by limiting the values of outliers based on
    their filled posts per bed ratio.

    This function is designed to identify outliers based on the ratio between
    filled posts and the number of beds at care homes and winsorize those
    outliers. Winsorization is the process of replacing outliers with a less
    extreme value. Non-care home data is not included in this particular filter
    so this part of the dataset will remain unchanged.

    Outlier detection - The number of beds at each location are banded into
    categorical groups and the average 'filled post per bed' ratio in each band
    is used to determine the 'expected filled posts' for each location. The
    residuals (the difference between actual and expected filled posts) are
    calculated, followed by the standardised residuals (residuals divided by the
    squart root of the filled post figure). The values at the top and bottom end
    of the standarised residuals are deemed to be outliers. The proportion of
    data to be identified as outliers is determined by the value of
    PERCENTAGE_OF_DATA_TO_REMOVE_AS_OUTLIERS.

    Winsorization - Filled post figures deemed outliers will be replaced by less
    extreme values calculated during the winsorization process.

    Args:
        input_lf (pl.LazyFrame): The input polars LazyFrame containing merged
            ASCWDS and CQC data.

    Returns:
        pl.LazyFrame: A polars LazyFrame with outlier values winsorized.
    """
    care_homes_lf = filter_lf_to_care_homes_with_known_beds_and_filled_posts(input_lf)
    data_not_relevant_to_filter_lf = select_data_not_in_subset(input_lf, care_homes_lf)

    avg_filled_posts_per_banded_bed_count_lf = (
        calculate_average_filled_posts_per_banded_bed_count(care_homes_lf)
    )

    care_homes_lf = calculate_expected_filled_posts_based_on_number_of_beds(
        care_homes_lf, avg_filled_posts_per_banded_bed_count_lf
    )

    care_homes_lf = calculate_filled_post_standardised_residual(care_homes_lf)

    care_homes_lf = calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
        care_homes_lf,
        SetValuesForWinsorization.PERCENTAGE_OF_DATA_TO_REMOVE_AS_OUTLIERS,
    )

    care_homes_lf = duplicate_ratios_within_standardised_residual_cutoffs(care_homes_lf)

    care_homes_lf = calculate_min_and_max_permitted_filled_posts_per_bed_ratios(
        care_homes_lf
    )

    winsorized_lf = winsorize_outliers(care_homes_lf)

    winsorized_lf = update_filtering_rule(
        winsorized_lf,
        IndCQC.ascwds_filtering_rule,
        IndCQC.ascwds_filled_posts_dedup,
        IndCQC.ascwds_filled_posts_dedup_clean,
        AscwdsFilteringRule.populated,
        AscwdsFilteringRule.winsorized_beds_ratio_outlier,
    )

    output_lf = combine_data(winsorized_lf, data_not_relevant_to_filter_lf)

    return output_lf


def filter_lf_to_care_homes_with_known_beds_and_filled_posts(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Filter LazyFrame to care homes with known beds and filled posts.

    This function filters the dataset to only include care homes with one or
    more beds and one or more filled posts.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame of cleaned CQC locations.

    Returns:
        pl.LazyFrame: A LazyFrame filtered to care homes with known beds and known
            filled posts.
    """
    lf = lf.filter(
        (pl.col(IndCQC.care_home) == CareHome.care_home)
        & pl.col(IndCQC.number_of_beds).is_not_null()
        & (pl.col(IndCQC.number_of_beds) > 0)
        & pl.col(IndCQC.ascwds_filled_posts_dedup_clean).is_not_null()
        & (pl.col(IndCQC.ascwds_filled_posts_dedup_clean) > 0.0)
    )

    return lf


def select_data_not_in_subset(
    complete_lf: pl.LazyFrame, subset_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Selects rows from the complete LazyFrame that are not present in the subset
    LazyFrame.

    Args:
        complete_lf (pl.LazyFrame): The LazyFrame containing all available data.
        subset_lf (pl.LazyFrame): The LazyFrame containing the subset of data to
            exclude.

    Returns:
        pl.LazyFrame: A new LazyFrame containing only the rows from complete_lf
            that are not in subset_lf.
    """
    output_lf = complete_lf.join(
        subset_lf, on=complete_lf.collect_schema().names(), how="anti"
    )

    return output_lf


def calculate_average_filled_posts_per_banded_bed_count(
    input_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Calculate the average filled posts per bed ratio for each banded bed count.

    This function groups the input LazyFrame by the number of banded beds and
    calculates the average filled posts per bed ratio for each group.

    Args:
        input_lf (pl.LazyFrame): A LazyFrame containing the columns
            'number_of_beds_banded' and 'filled_posts_per_bed_ratio'.

    Returns:
        pl.LazyFrame: A LazyFrame with the number of banded beds and the
            corresponding average filled posts per bed ratio.
    """
    output_lf = input_lf.group_by(IndCQC.number_of_beds_banded).agg(
        pl.col(IndCQC.filled_posts_per_bed_ratio)
        .mean()
        .alias(IndCQC.avg_filled_posts_per_bed_ratio)
    )

    return output_lf


def calculate_expected_filled_posts_based_on_number_of_beds(
    lf: pl.LazyFrame,
    join_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Calculates the expected number of filled posts based on the number of beds
    and average filled posts per bed ratio.

    This function joins the input LazyFrame with another LazyFrame containing
    the average filled posts per bed ratio for each banded bed count. It then
    calculates the expected number of filled posts for each row by multiplying
    the number of beds by the average filled posts per bed ratio.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing the columns 'number_of_beds'
            and 'number_of_beds_banded'.
        join_lf (pl.LazyFrame): A LazyFrame containing the columns
            'number_of_beds_banded' and 'avg_filled_posts_per_bed_ratio'.

    Returns:
        pl.LazyFrame: A LazyFrame with the additional column
        'expected_filled_posts'.
    """
    lf = lf.join(
        join_lf,
        on=IndCQC.number_of_beds_banded,
        how="left",
    )

    lf = lf.with_columns(
        (
            pl.col(IndCQC.number_of_beds)
            * pl.col(IndCQC.avg_filled_posts_per_bed_ratio)
        ).alias(IndCQC.expected_filled_posts)
    )

    return lf


def calculate_filled_post_standardised_residual(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Calculate the standardised residual for filled posts and adds as a new
    column.

    This function computes the standardised residual for filled posts by
    subtracting the expected filled posts from the actual filled posts
    (residuals) and then dividing by the square root of the expected filled
    posts. The result is added as a new column.

    Args:
        lf (pl.LazyFrame): LazyFrame containing
            'ascwds_filled_posts_dedup_clean'  and 'expected_filled_posts'.

    Returns:
        pl.LazyFrame: LazyFrame with the additional calculated
            'standardised_residual' column.
    """
    lf = lf.with_columns(
        (
            (
                pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
                - pl.col(IndCQC.expected_filled_posts)
            )
            / pl.col(IndCQC.expected_filled_posts).sqrt()
        ).alias(IndCQC.standardised_residual)
    )

    return lf


def calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
    lf: pl.LazyFrame,
    percentage_of_data_to_filter_out: float,
) -> pl.LazyFrame:
    """
    Calculates the lower and upper percentile cutoffs for standardised residuals
    in a LazyFrame and adds them as new columns.

    Calculates the lower and upper percentile cutoffs for standardised residuals
    in a LazyFrame. The value entered for the percentage_of_data_to_filter_out
    will be split into half so that half is applied to the lower extremes and
    half to the upper extremes. Two columns will be added as a result, one
    containing the lower percentile of standardised_residual and one containing
    the upper percentile of standardised_residual.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.
        percentage_of_data_to_filter_out (float): Percentage of data to filter out
            (eg, 0.05 will idenfity 5% of data as outliers).

    Returns:
        pl.LazyFrame: A LazyFrame with additional columns for both the lower and
            upper percentile values for standardised residuals.

    Raises:
        ValueError: If percentage of data provided is greater than 1 (equivalent
            to 100%).
    """
    if percentage_of_data_to_filter_out >= 1:
        raise ValueError(
            "Percentage of data to filter out must be less than 1 (equivalent to 100%)"
        )

    lower_percentile = percentage_of_data_to_filter_out / 2
    upper_percentile = 1 - lower_percentile

    lower_expr = (
        pl.col(IndCQC.standardised_residual)
        .quantile(lower_percentile, interpolation="linear")
        .over(IndCQC.primary_service_type)
    )

    upper_expr = (
        pl.col(IndCQC.standardised_residual)
        .quantile(upper_percentile, interpolation="linear")
        .over(IndCQC.primary_service_type)
    )

    return lf.with_columns(
        [
            lower_expr.alias(IndCQC.lower_percentile),
            upper_expr.alias(IndCQC.upper_percentile),
        ]
    )


def duplicate_ratios_within_standardised_residual_cutoffs(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Creates a column with the filled_posts_per_bed_ratio values when the
    standardised residuals are inside the percentile cutoffs.

    If the standardised_residual value is within the lower and upper percentile
    cutoffs then the filled_posts_per_bed_ratio value is duplicated into the new
    filled_posts_per_bed_ratio_within_std_resids column. Otherwise a null value
    is entered.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing
            filled_posts_per_bed_ratio, standardised residuals and percentiles.

    Returns:
        pl.LazyFrame: A LazyFrame with column
            filled_posts_per_bed_ratio_within_std_resids populated.
    """
    lf = lf.with_columns(
        pl.when(
            (pl.col(IndCQC.standardised_residual) >= pl.col(IndCQC.lower_percentile))
            & (pl.col(IndCQC.standardised_residual) <= pl.col(IndCQC.upper_percentile))
        )
        .then(pl.col(IndCQC.filled_posts_per_bed_ratio))
        .otherwise(pl.lit(None))
        .alias(IndCQC.filled_posts_per_bed_ratio_within_std_resids)
    )

    return lf


def calculate_min_and_max_permitted_filled_posts_per_bed_ratios(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Calculates the minimum and maximum permitted filled_posts_per_bed_ratio
    values and adds them as new columns.

    Outlier values in the filled_posts_per_bed_ratio_within_std_resids column
    have been nulled so this function will identify the minumum and maximum
    permitted ratios. Two columns will be added as a result, one containing the
    minimum filled_posts_per_bed_ratio and one containing the maximum
    filled_posts_per_bed_ratio.

    Following investigation with ASCWDS data alongside Capacity Tracker tracker
    data we were able to validate that the majority of ratios between 0.75 and
    5.0 were closely matched to each other. However, outside of these ratios the
    quality of matches decreased significantly. However, for small care homes
    (lower number of beds) the ratios then to be higher than 0.75 and 5.0 so we
    are using the standardised residual percentile cutoffs if they come out
    higher than these minimum permitted ratios.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with additional columns for both the min and max
            permitted filled_posts_per_bed_ratio.
    """
    min_expr = (
        pl.col(IndCQC.filled_posts_per_bed_ratio_within_std_resids)
        .min()
        .over(IndCQC.number_of_beds_banded)
    )

    max_expr = (
        pl.col(IndCQC.filled_posts_per_bed_ratio_within_std_resids)
        .max()
        .over(IndCQC.number_of_beds_banded)
    )

    min_with_cutoff = (
        pl.when(
            min_expr < SetValuesForWinsorization.MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF
        )
        .then(SetValuesForWinsorization.MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF)
        .otherwise(min_expr)
    )

    max_with_cutoff = (
        pl.when(
            max_expr < SetValuesForWinsorization.MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF
        )
        .then(SetValuesForWinsorization.MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF)
        .otherwise(max_expr)
    )

    return lf.with_columns(
        [
            min_with_cutoff.alias(IndCQC.min_filled_posts_per_bed_ratio),
            max_with_cutoff.alias(IndCQC.max_filled_posts_per_bed_ratio),
        ]
    )


def set_minimum_permitted_ratio(
    lf: pl.LazyFrame,
    column_name: str,
    minimum_permitted_ratio: float,
) -> pl.LazyFrame:
    """
    Replaces the value in the desired column with the minimum value of the
    current value or minimum_permitted_ratio.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        column_name (str): The name of the column to be modified.
        minimum_permitted_ratio (float): The minimum value that any entry in the
            column should have.

    Returns:
        pl.LazyFrame: A new LazyFrame with the specified column values adjusted
            to meet the minimum permitted ratio.
    """
    lf = lf.with_columns(
        pl.when(pl.col(column_name) < minimum_permitted_ratio)
        .then(minimum_permitted_ratio)
        .otherwise(pl.col(column_name))
        .alias(column_name)
    )

    return lf


def winsorize_outliers(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Replace ascwds_filled_posts_dedup_clean and filled_posts_per_bed_ratio with
    min or max permitted values.

    Outliers are detected using the filled_posts_per_bed_ratio. For ratios which
    fall outside of the minimum or maximum permitted ratios, the
    ascwds_filled_posts_dedup_clean is recalculated by multiplying the min/max
    permitted ratio by the number_of_beds. The filled_posts_per_bed_ratio is
    then winsorized.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with outliers winsorized.
    """
    ratio_below_min_condition = pl.col(IndCQC.filled_posts_per_bed_ratio) < pl.col(
        IndCQC.min_filled_posts_per_bed_ratio
    )
    min_permitted_filled_posts = pl.col(IndCQC.min_filled_posts_per_bed_ratio) * pl.col(
        IndCQC.number_of_beds
    )

    ratio_above_max_condition = pl.col(IndCQC.filled_posts_per_bed_ratio) > pl.col(
        IndCQC.max_filled_posts_per_bed_ratio
    )
    max_permitted_filled_posts = pl.col(IndCQC.max_filled_posts_per_bed_ratio) * pl.col(
        IndCQC.number_of_beds
    )

    winsorized_lf = lf.with_columns(
        pl.when(ratio_below_min_condition)
        .then(min_permitted_filled_posts)
        .when(ratio_above_max_condition)
        .then(max_permitted_filled_posts)
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .alias(IndCQC.ascwds_filled_posts_dedup_clean)
    )

    winsorized_lf = pUtils.calculate_filled_posts_per_bed_ratio(
        winsorized_lf,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.filled_posts_per_bed_ratio,
    )

    return winsorized_lf


def combine_data(
    filtered_care_home_lf: pl.LazyFrame, original_non_care_home_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Appends the filtered care home data back with the unfiltered non-care home data.

    This job filters care home data only so care home and non-care home data was separated at the start of the job.
    This function combines the two datasets back together. Only the columns which existed at the start of the job
    are required in the returned LazyFrame.

    Args:
        filtered_care_home_lf (pl.LazyFrame): A LazyFrame containing filtered care home data.
        original_non_care_home_lf (pl.LazyFrame): A LazyFrame containing the imported non-care home data.

    Returns:
        pl.LazyFrame: A new LazyFrame that combines the selected columns from filtered_care_home_df
        with the original_non_care_home_df.
    """
    care_home_lf = filtered_care_home_lf.select(
        original_non_care_home_lf.collect_schema().names()
    )

    all_locations_lf = pl.concat([original_non_care_home_lf, care_home_lf])

    return all_locations_lf
