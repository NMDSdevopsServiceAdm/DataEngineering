from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import Bucketizer

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


@dataclass
class NumericalValues:
    DECIMAL_PLACES_TO_ROUND_TO: int = 5
    PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS: float = 0.05
    MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF: float = 0.75
    MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF: float = 5.0


def winsorize_care_home_filled_posts_per_bed_ratio_outliers(
    input_df: DataFrame,
) -> DataFrame:
    """
    Winsorize ASCWDS filled posts by limiting the values of outliers based on their filled posts per bed ratio.

    This function is designed to identify outliers based on the ratio between filled posts and the number of beds at care
    homes and winsorize those outliers. Winsorization is the process of replacing outliers with a less extreme value.
    Non-care home data is not included in this particular filter so this part of the dataset will remain unchanged.

    Outlier detection:
        The number of beds at each location are banded into categorical groups and the average 'filled post per bed' ratio
        in each band is used to determine the 'expected filled posts' for each location. The residuals (the difference
        between actual and expected filled posts) are calculated, followed by the standardised residuals (residuals divided
        by the squart root of the filled post figure). The values at the top and bottom end of the standarised residuals
        are deemed to be outliers. The proportion of data to be identified as outliers is determined by the value of
        PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS.

    Winsorization:
        Filled post figures deemed outliers will be replaced by less extreme values calculated during the winsorization
        process.

    Args:
        df (DataFrame): The input dataframe containing merged ASCWDS and CQC data.

    Returns:
        DataFrame: A dataFrame with outlier values winsorized.
    """
    numerical_value = NumericalValues()

    care_homes_df = filter_df_to_care_homes_with_known_beds_and_filled_posts(input_df)
    data_not_relevant_to_filter_df = select_data_not_in_subset_df(
        input_df, care_homes_df
    )

    care_homes_df = create_banded_bed_count_column(care_homes_df)

    expected_filled_posts_per_banded_bed_count_df = (
        calculate_average_filled_posts_per_banded_bed_count(care_homes_df)
    )

    care_homes_df = calculate_expected_filled_posts_based_on_number_of_beds(
        care_homes_df, expected_filled_posts_per_banded_bed_count_df
    )

    care_homes_df = calculate_filled_post_standardised_residual(care_homes_df)

    care_homes_df = calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
        care_homes_df,
        numerical_value.PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS,
    )

    care_homes_df = duplicate_ratios_within_standardised_residual_cutoffs(care_homes_df)

    care_homes_df = calculate_min_and_max_permitted_filled_posts_per_bed_ratios(
        care_homes_df
    )

    # TODO: winsorize nulled values

    # TODO: this will need amending to account for those inside boundaries but
    filtered_care_home_df = update_filtering_rule(
        filtered_care_home_df,
        AscwdsFilteringRule.winsorized_care_home_filled_posts_to_bed_ratio_outlier,
    )
    output_df = combine_dataframes(
        filtered_care_home_df, data_not_relevant_to_filter_df
    )

    return output_df


def filter_df_to_care_homes_with_known_beds_and_filled_posts(
    df: DataFrame,
) -> DataFrame:
    """
    Filter dataframe to care homes with known beds and filled posts.

    This function filters to the dataset to only include care homes with one or more beds and
    one or more filled posts.

    Args:
        df (DataFrame): A dataframe of cleaned CQC locations.

    Returns:
        DataFrame: A dataframe filtered to care homes with known beds and known filled posts.
    """
    df = df.where(
        (F.col(IndCQC.care_home) == CareHome.care_home)
        & (
            F.col(IndCQC.number_of_beds).isNotNull()
            & (F.col(IndCQC.number_of_beds) > 0)
        )
        & (
            F.col(IndCQC.ascwds_filled_posts_dedup_clean).isNotNull()
            & (F.col(IndCQC.ascwds_filled_posts_dedup_clean) > 0.0)
        )
    )

    return df


def select_data_not_in_subset_df(
    complete_df: DataFrame, subset_df: DataFrame
) -> DataFrame:
    output_df = complete_df.exceptAll(subset_df)

    return output_df


def create_banded_bed_count_column(
    input_df: DataFrame,
) -> DataFrame:
    number_of_beds_df = input_df.select(IndCQC.number_of_beds).dropDuplicates()

    set_banded_boundaries = Bucketizer(
        splits=[0, 3, 5, 10, 15, 20, 25, 50, float("Inf")],
        inputCol=IndCQC.number_of_beds,
        outputCol=IndCQC.number_of_beds_banded,
    )

    number_of_beds_with_bands_df = set_banded_boundaries.setHandleInvalid(
        "keep"
    ).transform(number_of_beds_df)

    return input_df.join(number_of_beds_with_bands_df, IndCQC.number_of_beds, "left")


def calculate_average_filled_posts_per_banded_bed_count(
    input_df: DataFrame,
) -> DataFrame:
    output_df = input_df.groupBy(F.col(IndCQC.number_of_beds_banded)).agg(
        F.avg(IndCQC.filled_posts_per_bed_ratio).alias(
            IndCQC.avg_filled_posts_per_bed_ratio
        )
    )

    return output_df


def calculate_expected_filled_posts_based_on_number_of_beds(
    df: DataFrame,
    expected_filled_posts_per_banded_bed_count_df: DataFrame,
) -> DataFrame:
    df = df.join(
        expected_filled_posts_per_banded_bed_count_df,
        IndCQC.number_of_beds_banded,
        "left",
    )

    df = df.withColumn(
        IndCQC.expected_filled_posts,
        F.col(IndCQC.number_of_beds) * F.col(IndCQC.avg_filled_posts_per_bed_ratio),
    )

    return df


def calculate_filled_post_standardised_residual(
    df: DataFrame,
) -> DataFrame:
    """
    Calculate the standardised residual for filled posts and adds as a new column.

    This function computes the standardised residual for filled posts by subtracting the
    expected filled posts from the actual filled posts (residuals) and then dividing by
    the square root of the expected filled posts. The result is added as a new column.

    Args:
        df (DataFrame): DataFrame containing 'ascwds_filled_posts_dedup_clean' and
                       'expected_filled_posts'.

    Returns:
        DataFrame: DataFrame with the additional calculated 'standardised_residual' column.
    """
    df = df.withColumn(
        IndCQC.standardised_residual,
        (
            F.col(IndCQC.ascwds_filled_posts_dedup_clean)
            - F.col(IndCQC.expected_filled_posts)
        )
        / F.sqrt(F.col(IndCQC.expected_filled_posts)),
    )

    return df


def calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
    df: DataFrame,
    percentage_of_data_to_filter_out: float,
) -> DataFrame:
    """
    Calculates the lower and upper percentile cutoffs for standardised residuals in a DataFrame and adds them as new columns.

    Calculates the lower and upper percentile cutoffs for standardised residuals in a DataFrame. The value entered for the
    percentage_of_data_to_filter_out will be split into half so that half is applied to the lower extremes and half to the
    upper extremes. Two columns will be added as a result, one containing the lower percentile of standardised_residual and
    one containing the upper percentile of standardised_residual.

    Args:
        df (DataFrame): Input DataFrame.
        percentage_of_data_to_filter_out (float): Percentage of data to filter out (eg, 0.05 will idenfity 5% of data as
        outliers). Must be less than 1 (equivalent to 100%).

    Returns:
        DataFrame: DataFrame with additional columns for both the lower and upper percentile values for standardised residuals.
    """
    if percentage_of_data_to_filter_out >= 1:
        raise ValueError(
            "Percentage of data to filter out must be less than 1 (equivalent to 100%)"
        )

    lower_percentile = percentage_of_data_to_filter_out / 2
    upper_percentile = 1 - lower_percentile

    percentile_df = df.groupBy(IndCQC.primary_service_type).agg(
        F.expr(
            f"percentile({IndCQC.standardised_residual}, array({lower_percentile}))"
        )[0].alias(IndCQC.lower_percentile),
        F.expr(
            f"percentile({IndCQC.standardised_residual}, array({upper_percentile}))"
        )[0].alias(IndCQC.upper_percentile),
    )

    df = df.join(percentile_df, IndCQC.primary_service_type, "left")

    return df


def duplicate_ratios_within_standardised_residual_cutoffs(df: DataFrame) -> DataFrame:
    """
    Creates a column with the filled_posts_per_bed_ratio values when the standardised residuals are inside the percentile cutoffs.

    If the standardised_residual value is within the lower and upper percentile cutoffs then the filled_posts_per_bed_ratio value
    is duplicated into the new filled_posts_per_bed_ratio_within_std_resids column. Otherwise a null value is entered.

    Args:
        df (DataFrame): The input dataframe containing filled_posts_per_bed_ratio, standardised residuals and percentiles.

    Returns:
        DataFrame: A dataFrame with filled_posts_per_bed_ratio_within_std_resids populated.
    """
    df = df.withColumn(
        IndCQC.filled_posts_per_bed_ratio_within_std_resids,
        F.when(
            (F.col(IndCQC.standardised_residual) >= F.col(IndCQC.lower_percentile))
            & (F.col(IndCQC.standardised_residual) <= F.col(IndCQC.upper_percentile)),
            F.col(IndCQC.filled_posts_per_bed_ratio),
        ).otherwise(F.lit(None)),
    )

    return df


def calculate_min_and_max_permitted_filled_posts_per_bed_ratios(
    df: DataFrame,
) -> DataFrame:
    """
    Calculates the minimum and maximum permitted filled_posts_per_bed_ratio values and adds them as new columns.

    Outlier values in the filled_posts_per_bed_ratio_within_std_resids column have been nulled so this function
    will identify the minumum and maximum permitted ratios. Two columns will be added as a result, one containing
    the minimum filled_posts_per_bed_ratio and one containing the maximum filled_posts_per_bed_ratio.

    Following investigation with ASCWDS data alongside Capacity Tracker tracker data we were able to validate that
    the majority of ratio between 0.75 and 5.0 were closely matched to each other. However, outside of these ratio
    the quality of matches decreased significantly.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with additional columns for both the min and max permitted filled_posts_per_bed_ratio.
    """
    aggregated_df = df.groupBy(IndCQC.number_of_beds_banded).agg(
        F.min(IndCQC.filled_posts_per_bed_ratio_within_std_resids).alias(
            IndCQC.min_filled_posts_per_bed_ratio
        ),
        F.max(IndCQC.filled_posts_per_bed_ratio_within_std_resids).alias(
            IndCQC.max_filled_posts_per_bed_ratio
        ),
    )

    aggregated_df = set_minimum_permitted_ratio(
        aggregated_df,
        IndCQC.min_filled_posts_per_bed_ratio,
        NumericalValues.MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF,
    )
    aggregated_df = set_minimum_permitted_ratio(
        aggregated_df,
        IndCQC.max_filled_posts_per_bed_ratio,
        NumericalValues.MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF,
    )

    df = df.join(aggregated_df, IndCQC.number_of_beds_banded, "left")

    return df


def set_minimum_permitted_ratio(
    df: DataFrame,
    column_name: str,
    minimum_permitted_ratio: float,
) -> DataFrame:
    """
    Replaces the value in the desired column with the minimum value of the current value or minimum_permitted_ratio.

    Args:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column to be modified.
        minimum_permitted_ratio (float): The minimum value that any entry in the column should have.

    Returns:
        DataFrame: A new DataFrame with the specified column values adjusted to meet the minimum permitted ratio.
    """
    df = df.withColumn(
        column_name, F.greatest(F.col(column_name), F.lit(minimum_permitted_ratio))
    )

    return df


def combine_dataframes(
    filtered_care_home_df: DataFrame, original_non_care_home_df: DataFrame
) -> DataFrame:
    """
    Appends the filtered care home data back with the unfiltered non-care home data.

    This job filters care home data only so care home and non-care home data was separated at the start of the job.
    This function combines the two datasets back together. Only the columns which existed at the start of the job
    are required in the returned dataframe.

    Args:
        filtered_care_home_df (DataFrame): A DataFrame containing filtered care home data.
        original_non_care_home_df (DataFrame): A DataFrame containing the imported non-care home data.

    Returns:
        DataFrame: A new DataFrame that combines the selected columns from filtered_care_home_df
        with the original_non_care_home_df.
    """
    care_home_df = filtered_care_home_df.select(original_non_care_home_df.columns)

    all_locations_df = original_non_care_home_df.unionByName(care_home_df)

    return all_locations_df
