from dataclasses import dataclass

from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.models.utils import (
    insert_predictions_into_pipeline,
    set_min_value,
)


@dataclass
class TempColumns:
    """Temporary columns used in the adjustment process."""

    adjustment_ratio: str = "adjustment_ratio"
    avg_with_dormancy: str = "avg_with_dormancy"
    avg_without_dormancy: str = "avg_without_dormancy"
    first_overlap_date: str = "first_overlap_date"
    residual_at_overlap: str = "residual_at_overlap"
    adjusted_without_dormancy_model: str = "adjusted_without_dormancy_model"
    adjusted_and_residual_applied_without_dormancy_model: str = (
        "adjusted_and_residual_applied_without_dormancy_model"
    )
    time_registered_banded_and_capped: str = "time_registered_banded_and_capped"


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
    locations_reduced_df = locations_df.select(
        IndCqc.location_id,
        IndCqc.cqc_location_import_date,
        IndCqc.care_home,
        IndCqc.related_location,
        IndCqc.time_registered,
        IndCqc.non_res_without_dormancy_model,
        IndCqc.non_res_with_dormancy_model,
    )

    non_res_locations_df = utils.select_rows_with_value(
        locations_reduced_df, IndCqc.care_home, CareHome.not_care_home
    )

    non_res_locations_df = group_time_registered_to_six_month_bands(
        non_res_locations_df
    )

    combined_models_df = calculate_and_apply_model_ratios(non_res_locations_df)

    combined_models_df = calculate_and_apply_residuals(combined_models_df)

    combined_models_df = combine_model_predictions(combined_models_df)

    combined_models_df = set_min_value(combined_models_df, IndCqc.prediction, 1.0)

    locations_with_predictions_df = insert_predictions_into_pipeline(
        locations_df,
        combined_models_df,
        IndCqc.non_res_combined_model,
    )

    return locations_with_predictions_df


def group_time_registered_to_six_month_bands(df: DataFrame) -> DataFrame:
    """
    Groups 'time_registered' into six-month bands and caps values beyond ten years.

    The 'time_registered' column starts at 1 for individuals registered for up to one month.
    To align with six-month bands where '1' represents up to six months, we subtract 1 before
    dividing by 6.

    Since registration patterns change little beyond ten years and sample sizes decrease,
    we cap values at 10 years (equivalent to 20 six-month bands) for consistency.

    Args:
        df (DataFrame): Input DataFrame containing the 'time_registered' column.

    Returns:
        DataFrame: DataFrame with 'time_registered' transformed into six-month bands,
                   capped at ten years.
    """
    six_month_time_bands: int = 6
    ten_years: int = 20  # 10 years is 20 six-month bands

    df = df.withColumn(
        TempColumns.time_registered_banded_and_capped,
        F.floor(
            (F.col(IndCqc.time_registered) - F.lit(1)) / F.lit(six_month_time_bands)
        ),
    )
    df = df.withColumn(
        TempColumns.time_registered_banded_and_capped,
        F.least(
            F.col(TempColumns.time_registered_banded_and_capped),
            F.lit(ten_years),
        ),
    )
    return df


def calculate_and_apply_model_ratios(df: DataFrame) -> DataFrame:
    """
    Calculates the ratio between 'with_dormancy' and 'without_dormancy' models by partitioning
    the dataset based on 'related_location' and 'time_registered_banded_and_capped'.

    Args:
        df (DataFrame): Input DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with partition-level ratios applied.
    """
    ratio_df = average_models_by_related_location_and_time_registered(df)

    ratio_df = calculate_adjustment_ratios(ratio_df)

    df = df.join(
        ratio_df,
        [IndCqc.related_location, TempColumns.time_registered_banded_and_capped],
        "left",
    )

    df = apply_model_ratios(df)

    return df


def average_models_by_related_location_and_time_registered(df: DataFrame) -> DataFrame:
    """
    Averages model predictions by 'related_location' and 'time_registered_banded_and_capped'.

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
        IndCqc.related_location, TempColumns.time_registered_banded_and_capped
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
        TempColumns.adjusted_without_dormancy_model,
        F.col(IndCqc.non_res_without_dormancy_model)
        * F.col(TempColumns.adjustment_ratio),
    )

    return df


def get_first_overlap_date(df: DataFrame) -> DataFrame:
    """
    Identifies the first available date where both models exist for each location.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with 'first_overlap_date' added.
    """
    window_spec = Window.partitionBy(IndCqc.location_id).orderBy(
        IndCqc.cqc_location_import_date
    )

    df = df.withColumn(
        TempColumns.first_overlap_date,
        F.first(
            F.when(
                F.col(IndCqc.non_res_with_dormancy_model).isNotNull(),
                F.col(IndCqc.cqc_location_import_date),
            ),
            True,
        ).over(window_spec),
    )
    return df


def calculate_residuals(df: DataFrame) -> DataFrame:
    """
    Calculates residuals between 'with_dormancy' and 'adjusted_without_dormancy' models at the first overlap date.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with calculated residuals.
    """
    residual_df = (
        df.filter(
            F.col(IndCqc.cqc_location_import_date)
            == F.col(TempColumns.first_overlap_date)
        )
        .withColumn(
            TempColumns.residual_at_overlap,
            F.col(IndCqc.non_res_with_dormancy_model)
            - F.col(TempColumns.adjusted_without_dormancy_model),
        )
        .select(IndCqc.location_id, TempColumns.residual_at_overlap)
    )
    return residual_df


def apply_residuals(df: DataFrame) -> DataFrame:
    """
    Applies the residuals to smooth predictions.

    Args:
        df (DataFrame): DataFrame with model predictions and residuals.

    Returns:
        DataFrame: DataFrame with smoothed predictions.
    """
    df = df.withColumn(
        TempColumns.adjusted_and_residual_applied_without_dormancy_model,
        F.when(
            F.col(TempColumns.residual_at_overlap).isNotNull(),
            F.col(TempColumns.adjusted_without_dormancy_model)
            + F.col(TempColumns.residual_at_overlap),
        ).otherwise(F.col(TempColumns.adjusted_without_dormancy_model)),
    )
    return df


def calculate_and_apply_residuals(df: DataFrame) -> DataFrame:
    """
    Calculates and applies residuals between models to smooth predictions.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with smoothed predictions.
    """
    df = get_first_overlap_date(df)

    residual_df = calculate_residuals(df)

    df = df.join(residual_df, IndCqc.location_id, "left")

    df = apply_residuals(df)

    return df


def combine_model_predictions(df: DataFrame) -> DataFrame:
    """
    Uses the 'with_dormancy' model predictions where available, otherwise uses the adjusted 'without_dormancy' model predictions.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with combined model predictions.
    """
    df = df.withColumn(
        IndCqc.prediction,
        F.coalesce(
            IndCqc.non_res_with_dormancy_model,
            TempColumns.adjusted_and_residual_applied_without_dormancy_model,
        ),
    )

    return df
