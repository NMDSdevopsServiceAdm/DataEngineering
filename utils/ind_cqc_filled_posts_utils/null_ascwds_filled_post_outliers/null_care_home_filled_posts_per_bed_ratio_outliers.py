from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import Bucketizer
from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


@dataclass
class TempColNames:
    filled_posts_per_bed_ratio: str = "filled_posts_per_bed_ratio"
    avg_filled_posts_per_bed_ratio: str = "avg_filled_posts_per_bed_ratio"
    number_of_beds_banded: str = "number_of_beds_banded"
    residual: str = "residual"
    standardised_residual: str = "standardised_residual"
    expected_filled_posts: str = "expected_filled_posts"
    lower_percentile: str = "lower_percentile"
    upper_percentile: str = "upper_percentile"


@dataclass
class NumericalValues:
    DECIMAL_PLACES_TO_ROUND_TO: int = 5
    PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS: float = 0.05


def null_care_home_filled_posts_per_bed_ratio_outliers(
    input_df: DataFrame,
) -> DataFrame:
    """
    This function is designed to convert filled post figures for care homes which are deemed outliers to
    null based on the ratio between filled posts and number of beds. The number of beds are banded into
    categorical groups and the average 'filled post per bed' ratio is calculated which becomes the ratio
    to determine the 'expected filled posts' for each banded number of bed group. The residuals (the
    difference between actual and expected) are calculated, followed by the standardised residuals
    (residuals divided by the squart root of the filled post figure). The values at the top and bottom
    end of the standarised residuals are deemed to be outliers (based on percentiles) and the filled post
    figures in ascwds_filled_posts_clean are converted to null values. Non-care home data is not included
    in this particular filter so this part of the dataframe will be unchanged.

    Args:
        df (DataFrame): The input dataframe containing merged ASCWDS and CQC data.

    Returns:
        DataFrame: A dataFrame with outlier values converted to null values.
    """
    numerical_value = NumericalValues()

    care_homes_df = select_relevant_data(input_df)
    data_not_relevant_to_filter_df = select_data_not_in_subset_df(
        input_df, care_homes_df
    )

    data_to_filter_df = calculate_filled_posts_per_bed_ratio(care_homes_df)

    data_to_filter_df = create_banded_bed_count_column(data_to_filter_df)

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

    return output_df


def select_relevant_data(input_df: DataFrame) -> DataFrame:
    output_df = input_df.where((F.col(IndCQC.care_home) == "Y"))
    output_df = output_df.where(
        F.col(IndCQC.number_of_beds).isNotNull() & (F.col(IndCQC.number_of_beds) > 0)
    )
    output_df = output_df.where(
        F.col(IndCQC.ascwds_filled_posts_clean).isNotNull()
        & (F.col(IndCQC.ascwds_filled_posts_clean) > 0.0)
    )

    return output_df


def select_data_not_in_subset_df(
    complete_df: DataFrame, subset_df: DataFrame
) -> DataFrame:
    output_df = complete_df.exceptAll(subset_df)

    return output_df


def calculate_filled_posts_per_bed_ratio(
    input_df: DataFrame,
) -> DataFrame:
    input_df = input_df.withColumn(
        TempColNames.filled_posts_per_bed_ratio,
        F.col(IndCQC.ascwds_filled_posts_clean) / F.col(IndCQC.number_of_beds),
    )

    return input_df


def create_banded_bed_count_column(
    input_df: DataFrame,
) -> DataFrame:
    number_of_beds_df = input_df.select(IndCQC.number_of_beds).dropDuplicates()

    set_banded_boundaries = Bucketizer(
        splits=[0, 3, 5, 10, 15, 20, 25, 50, float("Inf")],
        inputCol=IndCQC.number_of_beds,
        outputCol=TempColNames.number_of_beds_banded,
    )

    number_of_beds_with_bands_df = set_banded_boundaries.setHandleInvalid(
        "keep"
    ).transform(number_of_beds_df)

    return input_df.join(number_of_beds_with_bands_df, IndCQC.number_of_beds, "left")


def calculate_average_filled_posts_per_banded_bed_count(
    input_df: DataFrame,
) -> DataFrame:
    output_df = input_df.groupBy(F.col(TempColNames.number_of_beds_banded)).agg(
        F.avg(TempColNames.filled_posts_per_bed_ratio).alias(
            TempColNames.avg_filled_posts_per_bed_ratio
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
        TempColNames.number_of_beds_banded,
        "left",
    )

    df = df.withColumn(
        TempColNames.expected_filled_posts,
        F.col(IndCQC.number_of_beds)
        * F.col(TempColNames.avg_filled_posts_per_bed_ratio),
    )

    return df


def calculate_filled_post_residuals(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        TempColNames.residual,
        F.col(IndCQC.ascwds_filled_posts_clean)
        - F.col(TempColNames.expected_filled_posts),
    )

    return df


def calculate_filled_post_standardised_residual(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        TempColNames.standardised_residual,
        F.col(TempColNames.residual)
        / F.sqrt(F.col(TempColNames.expected_filled_posts)),
    )

    return df


def calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
    df: DataFrame,
    percentage_of_data_to_filter_out: float,
) -> DataFrame:
    """
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
        F.percentile_approx(
            TempColNames.standardised_residual, lower_percentile, F.lit(1000000)
        ).alias(TempColNames.lower_percentile),
        F.percentile_approx(
            TempColNames.standardised_residual, upper_percentile, F.lit(1000000)
        ).alias(TempColNames.upper_percentile),
    )

    df = df.join(percentile_df, IndCQC.primary_service_type, "left")

    return df


def null_values_outside_of_standardised_residual_cutoffs(
    df: DataFrame,
) -> DataFrame:
    """
    If the standardised_residual value is outside of the lower and upper percentile cutoffs then
    ascwds_filled_posts_clean is replaced with a null value. Otherwise (if the value is within
    the cutoffs), the original value for ascwds_filled_posts_clean remains. The function used to
    calculate percentiles (percentile_approx)

    Args:
        df (DataFrame): The input dataframe containing standardised residuals and percentiles.

    Returns:
        DataFrame: A dataFrame with null values removed based on the specified cutoffs.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_clean,
        F.when(
            (
                F.col(TempColNames.standardised_residual)
                <= F.col(TempColNames.lower_percentile)
            )
            | (
                F.col(TempColNames.standardised_residual)
                > F.col(TempColNames.upper_percentile)
            ),
            F.lit(None),
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_clean)),
    )

    return df


def combine_dataframes(
    filtered_care_home_df: DataFrame, original_non_care_home_df: DataFrame
) -> DataFrame:
    """
    This job filteres care home data only so care home and non-care home data was separated at the start of the job.
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
