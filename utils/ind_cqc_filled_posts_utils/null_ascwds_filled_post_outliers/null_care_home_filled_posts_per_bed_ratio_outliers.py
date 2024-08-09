from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import Bucketizer

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


@dataclass
class NumericalValues:
    DECIMAL_PLACES_TO_ROUND_TO: int = 5
    PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS: float = 0.1


def null_care_home_filled_posts_per_bed_ratio_outliers(
    input_df: DataFrame,
) -> DataFrame:
    """
    Converts filled post values to nulls if they are outliers based on their filled posts per bed ratio.

    This function is designed to convert filled post figures for care homes which are deemed outliers to
    null based on the ratio between filled posts and number of beds. The number of beds are banded into
    categorical groups and the average 'filled post per bed' ratio is calculated which becomes the ratio
    to determine the 'expected filled posts' for each banded number of bed group. The residuals (the
    difference between actual and expected) are calculated, followed by the standardised residuals
    (residuals divided by the squart root of the filled post figure). The values at the top and bottom
    end of the standarised residuals are deemed to be outliers (based on percentiles) and the filled post
    figures in ascwds_filled_posts_dedup_clean are converted to null values. Non-care home data is not included
    in this particular filter so this part of the dataframe will be unchanged.

    Args:
        df (DataFrame): The input dataframe containing merged ASCWDS and CQC data.

    Returns:
        DataFrame: A dataFrame with outlier values converted to null values.
    """
    numerical_value = NumericalValues()

    care_homes_df = filter_df_to_care_homes_with_known_beds_and_filled_posts(input_df)
    data_not_relevant_to_filter_df = select_data_not_in_subset_df(
        input_df, care_homes_df
    )

    data_to_filter_df = create_banded_bed_count_column(care_homes_df)

    expected_filled_posts_per_banded_bed_count_df = (
        calculate_average_filled_posts_per_banded_bed_count(data_to_filter_df)
    )

    data_to_filter_df = calculate_standardised_residuals(
        data_to_filter_df, expected_filled_posts_per_banded_bed_count_df
    )

    data_to_filter_df = (
        calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
            data_to_filter_df,
            numerical_value.PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS,
        )
    )

    filtered_care_home_df = null_values_outside_of_standardised_residual_cutoffs(
        data_to_filter_df
    )

    output_df = combine_dataframes(
        filtered_care_home_df, data_not_relevant_to_filter_df
    )

    output_df = update_filtering_rule(
        output_df,
        AscwdsFilteringRule.filtered_care_home_filled_posts_to_bed_ratio_outlier,
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
        (DataFrame): A dataframe filtered to care homes with known beds and known filled posts.
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


def calculate_standardised_residuals(
    df: DataFrame,
    expected_filled_posts_per_banded_bed_count_df: DataFrame,
) -> DataFrame:
    df = calculate_expected_filled_posts_based_on_number_of_beds(
        df, expected_filled_posts_per_banded_bed_count_df
    )
    df = calculate_filled_post_residuals(df)
    df = calculate_filled_post_standardised_residual(df)

    return df


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


def calculate_filled_post_residuals(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        IndCQC.residual,
        F.col(IndCQC.ascwds_filled_posts_dedup_clean)
        - F.col(IndCQC.expected_filled_posts),
    )

    return df


def calculate_filled_post_standardised_residual(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        IndCQC.standardised_residual,
        F.col(IndCQC.residual) / F.sqrt(F.col(IndCQC.expected_filled_posts)),
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


def null_values_outside_of_standardised_residual_cutoffs(
    df: DataFrame,
) -> DataFrame:
    """
    Converts filled post values to null if the standardised residuals are outside the percentile cutoffs.

    If the standardised_residual value is outside of the lower and upper percentile cutoffs then
    ascwds_filled_posts_dedup_clean is replaced with a null value. Otherwise (if the value is within the
    cutoffs), the original value for ascwds_filled_posts_dedup_clean remains.

    Args:
        df (DataFrame): The input dataframe containing standardised residuals and percentiles.

    Returns:
        DataFrame: A dataFrame with null values removed based on the specified cutoffs.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            (F.col(IndCQC.standardised_residual) < F.col(IndCQC.lower_percentile))
            | (F.col(IndCQC.standardised_residual) > F.col(IndCQC.upper_percentile)),
            F.lit(None),
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
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
