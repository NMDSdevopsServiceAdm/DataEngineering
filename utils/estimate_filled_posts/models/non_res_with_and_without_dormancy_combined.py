from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    NonResWithAndWithoutDormancyCombinedColumns as TempColumns,
)
from utils.column_values.categorical_column_values import CareHome


def combine_non_res_with_and_without_dormancy_models(
    locations_df: DataFrame,
) -> DataFrame:
    """
    Creates a combined model prediction by adjusting the 'without_dormancy' model to align with
    the 'with_dormancy' model and applying residual corrections for smoothing.

    Args:
        locations_df (DataFrame): Input DataFrame containing 'without_dormancy' and 'with_dormancy' model predictions.

    Returns:
        DataFrame: The original DataFrame with the combined model predictions joined in.
    """
    locations_df = locations_df.select(
        IndCqc.location_id,
        IndCqc.cqc_location_import_date,
        IndCqc.related_location,
        IndCqc.time_registered,
        IndCqc.non_res_without_dormancy_model,
        IndCqc.non_res_with_dormancy_model,
    )

    non_res_locations_df = utils.select_rows_with_value(
        locations_df, IndCqc.care_home, CareHome.not_care_home
    )

    non_res_locations_df = calculate_and_apply_model_ratios(non_res_locations_df)

    # TODO - 3 - calculate and apply residuals

    # TODO - 4 - combine model predictions

    # TODO - 5 - set_min_value and insert predictions into pipeline

    return non_res_locations_df  # TODO add tests


# TODO add tests
def calculate_and_apply_model_ratios(df: DataFrame) -> DataFrame:
    """
    Calculates the ratio between 'with_dormancy' and 'without_dormancy' models by partitioning
    the dataset based on 'related_location' and 'time_registered'.

    Args:
        df (DataFrame): Input DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with partition-level ratios applied.
    """
    ratio_df = average_models_by_related_location_and_time_registered(df)

    ratio_df = calculate_adjustment_ratios(ratio_df)

    df = df.join(ratio_df, [IndCqc.related_location, IndCqc.time_registered], "left")

    df = apply_model_ratios(df)

    return df


def average_models_by_related_location_and_time_registered(df: DataFrame) -> DataFrame:
    """
    Averages model predictions by 'related_location' and 'time_registered'.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with averaged model predictions.
    """
    both_models_known_df = df.where(
        F.col(IndCqc.non_res_with_dormancy_model).isNotNull()
        & F.col(IndCqc.non_res_without_dormancy_model).isNotNull()
    )

    avg_df = both_models_known_df.groupBy(
        IndCqc.related_location, IndCqc.time_registered
    ).agg(
        F.avg(IndCqc.non_res_with_dormancy_model).alias(TempColumns.avg_with_dormancy),
        F.avg(IndCqc.non_res_without_dormancy_model).alias(
            TempColumns.avg_without_dormancy
        ),
    )
    return avg_df


# TODO add tests
def calculate_adjustment_ratios(df: DataFrame) -> DataFrame:
    """
    Calculates the adjustment ratio between 'with_dormancy' and 'without_dormancy' models.

    Args:
        df (DataFrame): DataFrame with aggregated model predictions.

    Returns:
        DataFrame: DataFrame with adjustment ratios calculated.
    """
    df = df.withColumn(
        TempColumns.adjustment_ratio,
        F.when(
            F.col(TempColumns.avg_without_dormancy) != 0,
            F.col(TempColumns.avg_with_dormancy)
            / F.col(TempColumns.avg_without_dormancy),
        ).otherwise(1.0),
    )
    return df


# TODO add tests
def apply_model_ratios(df: DataFrame) -> DataFrame:
    """
    Applies the adjustment ratio to 'without_dormancy' model predictions.

    Args:
        df (DataFrame): DataFrame with model predictions and adjustment ratios.

    Returns:
        DataFrame: DataFrame with adjusted 'without_dormancy' model predictions.
    """
    df = df.withColumn(
        TempColumns.adjusted_without_dormancy_model,
        F.col(IndCqc.non_res_without_dormancy_model)
        * F.col(TempColumns.adjustment_ratio),
    )

    return df
