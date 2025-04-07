from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    NonResWithAndWithoutDormancyCombinedColumns as TempColumns,
)
from utils.column_values.categorical_column_values import CareHome
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


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
        IndCqc.care_home,
        IndCqc.related_location,
        IndCqc.time_registered,
        IndCqc.non_res_without_dormancy_model,
        IndCqc.non_res_with_dormancy_model,
    )

    non_res_locations_df = utils.select_rows_with_value(
        locations_df, IndCqc.care_home, CareHome.not_care_home
    )

    # TODO - 6 - convert time registered month bands to 6 monthly bands (capped at 10 years)

    combined_models_df = calculate_and_apply_model_ratios(non_res_locations_df)

    combined_models_df = calculate_and_apply_residuals(combined_models_df)

    # TODO - 4 - combine model predictions

    # TODO - 5 - set_min_value and insert predictions into pipeline

    return non_res_locations_df  # TODO add tests


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


def calculate_adjustment_ratios(df: DataFrame) -> DataFrame:
    """
    Calculates the adjustment ratio between 'with_dormancy' and 'without_dormancy' models.

    If the avg_without_dormancy is not zero, the adjustment ratio is calculated as the ratio of
    avg_with_dormancy to avg_without_dormancy. Otherwise, the adjustment ratio is set to 1.0 (no change).

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


def apply_model_ratios(df: DataFrame) -> DataFrame:
    """
    Applies the adjustment ratio to 'without_dormancy' model predictions.

    Args:
        df (DataFrame): DataFrame with model predictions and adjustment ratios.

    Returns:
        DataFrame: DataFrame with adjusted 'without_dormancy' model predictions.
    """
    df = df.withColumn(
        TempColumns.without_dormancy_model_adjusted,
        F.col(IndCqc.non_res_without_dormancy_model)
        * F.col(TempColumns.adjustment_ratio),
    )

    return df


def calculate_and_apply_residuals(df: DataFrame) -> DataFrame:
    """
    Calculates and applies residuals between models at the first point in time when both models exist to smooth predictions.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with smoothed predictions.
    """
    window_spec = Window.partitionBy(IndCqc.location_id).orderBy(
        IndCqc.cqc_location_import_date
    )
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values=IndCqc.non_res_with_dormancy_model,
        column_with_data=IndCqc.cqc_location_import_date,
        new_column=TempColumns.first_overlap_date,
        selection="first",
    )

    residual_df = calculate_residuals(df)

    df = df.join(residual_df, IndCqc.location_id, "left")

    df = apply_residuals(df)

    return df


def calculate_residuals(df: DataFrame) -> DataFrame:
    """
    Calculates residuals between 'with_dormancy' and 'without_dormancy_model_adjusted' models at the first overlap date.

    This function filters the DataFrame at the first point in time when the 'with_dormancy_model' has values, and to locations
    who have both a 'with_dormancy' and 'without_dormancy_adjusted' model prediction.
    The residual is calculated as 'with_dormancy' values minus 'without_dormancy_adjusted'.
    Only the columns 'location_id' and 'residual_at_overlap' are returned.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with 'location_id' and 'residual_at_overlap'.
    """
    filtered_df = df.where(
        (
            F.col(IndCqc.cqc_location_import_date)
            == F.col(TempColumns.first_overlap_date)
        )
        & F.col(IndCqc.non_res_with_dormancy_model).isNotNull()
        & F.col(TempColumns.without_dormancy_model_adjusted).isNotNull()
    )
    residual_df = filtered_df.withColumn(
        TempColumns.residual_at_overlap,
        F.col(IndCqc.non_res_with_dormancy_model)
        - F.col(TempColumns.without_dormancy_model_adjusted),
    )
    residual_df = residual_df.select(
        IndCqc.location_id, TempColumns.residual_at_overlap
    )

    return residual_df


def apply_residuals(df: DataFrame) -> DataFrame:
    """
    Applies the residuals to smooth predictions when both the model value and residual are not null.

    Args:
        df (DataFrame): DataFrame with model predictions and residuals.

    Returns:
        DataFrame: DataFrame with smoothed predictions.
    """
    model_and_residual_are_not_null = (
        F.col(TempColumns.without_dormancy_model_adjusted).isNotNull()
        & F.col(TempColumns.residual_at_overlap).isNotNull()
    )

    df = df.withColumn(
        TempColumns.without_dormancy_model_adjusted_and_residual_applied,
        F.when(
            model_and_residual_are_not_null,
            F.col(TempColumns.without_dormancy_model_adjusted)
            + F.col(TempColumns.residual_at_overlap),
        ).otherwise(F.col(TempColumns.without_dormancy_model_adjusted)),
    )
    return df
