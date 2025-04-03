from pyspark.sql import DataFrame, Window, functions as F

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

    non_res_locations_df = calculate_and_apply_model_ratios(non_res_locations_df)

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

    Args:
        df (DataFrame): DataFrame with aggregated model predictions.

    Returns:
        DataFrame: DataFrame with adjustment ratios calculated.
    """
    df = df.withColumn(
        TempColumns.adjustment_ratio,
        F.col(TempColumns.avg_with_dormancy) / F.col(TempColumns.avg_without_dormancy),
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
    Calculates residuals between 'with_dormancy' and 'without_dormancy_model_adjusted' models at the first overlap date.

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
            - F.col(TempColumns.without_dormancy_model_adjusted),
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
        TempColumns.without_dormancy_model_adjusted_and_residual_applied,
        F.when(
            F.col(TempColumns.residual_at_overlap).isNotNull(),
            F.col(TempColumns.without_dormancy_model_adjusted)
            + F.col(TempColumns.residual_at_overlap),
        ).otherwise(F.col(TempColumns.without_dormancy_model_adjusted)),
    )
    return df
