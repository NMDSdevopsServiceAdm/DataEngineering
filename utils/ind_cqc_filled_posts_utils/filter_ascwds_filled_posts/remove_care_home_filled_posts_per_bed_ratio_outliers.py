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


def remove_care_home_filled_posts_per_bed_ratio_outliers(
    input_df: DataFrame,
) -> DataFrame:
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

    care_homes_within_standardised_residual_cutoff_df = (
        create_filled_posts_clean_col_in_filtered_df(data_to_filter_df)
    )

    care_homes_with_filtered_col_df = join_filtered_col_into_care_home_df(
        care_homes_df, care_homes_within_standardised_residual_cutoff_df
    )

    data_not_relevant_to_filter_df = (
        add_filled_posts_clean_without_filtering_to_data_outside_of_this_filter(
            data_not_relevant_to_filter_df
        )
    )

    output_df = combine_dataframes(
        care_homes_with_filtered_col_df, data_not_relevant_to_filter_df
    )

    return output_df


def select_relevant_data(input_df: DataFrame) -> DataFrame:
    output_df = input_df.where((F.col(IndCQC.care_home) == "Y"))
    output_df = output_df.where(
        F.col(IndCQC.number_of_beds).isNotNull() & (F.col(IndCQC.number_of_beds) > 0)
    )
    output_df = output_df.where(
        F.col(IndCQC.ascwds_filled_posts).isNotNull()
        & (F.col(IndCQC.ascwds_filled_posts) > 0.0)
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
        F.col(IndCQC.ascwds_filled_posts) / F.col(IndCQC.number_of_beds),
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
        F.col(IndCQC.ascwds_filled_posts) - F.col(TempColNames.expected_filled_posts),
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
        F.expr(
            f"percentile({TempColNames.standardised_residual}, array({lower_percentile}))"
        )[0].alias(TempColNames.lower_percentile),
        F.expr(
            f"percentile({TempColNames.standardised_residual}, array({upper_percentile}))"
        )[0].alias(TempColNames.upper_percentile),
    )

    df = df.join(percentile_df, IndCQC.primary_service_type, "left")

    return df


def create_filled_posts_clean_col_in_filtered_df(
    df: DataFrame,
) -> DataFrame:
    within_boundary_df = df.filter(
        (
            F.col(TempColNames.standardised_residual)
            > F.col(TempColNames.lower_percentile)
        )
        & (
            F.col(TempColNames.standardised_residual)
            < F.col(TempColNames.upper_percentile)
        )
    )

    within_boundary_df = within_boundary_df.withColumn(
        IndCQC.ascwds_filled_posts_clean, F.col(IndCQC.ascwds_filled_posts)
    )

    output_df = within_boundary_df.select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.ascwds_filled_posts_clean,
    )

    return output_df


def join_filtered_col_into_care_home_df(
    df: DataFrame, df_with_filtered_column: DataFrame
) -> DataFrame:
    df = df.join(
        df_with_filtered_column,
        [IndCQC.location_id, IndCQC.cqc_location_import_date],
        "left",
    )
    return df


def add_filled_posts_clean_without_filtering_to_data_outside_of_this_filter(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_clean, F.col(IndCQC.ascwds_filled_posts)
    )
    return df


def combine_dataframes(first_df: DataFrame, second_df: DataFrame) -> DataFrame:
    output_df = first_df.unionByName(second_df)

    return output_df
