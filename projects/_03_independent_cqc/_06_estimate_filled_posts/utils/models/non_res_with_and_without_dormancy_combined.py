from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    NonResWithAndWithoutDormancyCombinedColumns as TempColumns,
)
from utils.column_values.categorical_column_values import CareHome
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils import (
    insert_predictions_into_pipeline,
    set_min_value,
)
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

    combined_models_df = group_time_registered_to_six_month_bands(non_res_locations_df)

    combined_models_df = calculate_and_apply_model_ratios(combined_models_df)

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

    The 'time_registered' column starts at one for individuals registered for up to one month.
    To align with six-month bands where '1' represents up to six months, we subtract one before
    dividing by six.

    Since registration patterns change little beyond ten years and sample sizes decrease,
    we cap values at ten years (equivalent to 20 six-month bands) for consistency.

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

    Only model predictions for locations who have a non null for both models contribute to the average.

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
        TempColumns.non_res_without_dormancy_model_adjusted,
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
    first_overlap_df = get_selected_value(
        df,
        window_spec,
        column_with_null_values=IndCqc.non_res_with_dormancy_model,
        column_with_data=IndCqc.cqc_location_import_date,
        new_column=TempColumns.first_overlap_date,
        selection="first",
    )

    residual_df = calculate_residuals(first_overlap_df)

    df = df.join(residual_df, IndCqc.location_id, "left")

    df = apply_residuals(df)

    return df


def calculate_residuals(df: DataFrame) -> DataFrame:
    """
    Calculates residuals between 'with_dormancy' and 'non_res_without_dormancy_model_adjusted' models at the first overlap date.

    This function filters the DataFrame at the first point in time ('first_overlap_date') when the 'with_dormancy_model' has values,
    and to locations who have both a 'with_dormancy' and 'without_dormancy_adjusted' model prediction.
    This is used elsewhere in the code to smooth out the predictions at the point in time when one model switches the other.

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
        & F.col(TempColumns.non_res_without_dormancy_model_adjusted).isNotNull()
    )
    residual_df = filtered_df.withColumn(
        TempColumns.residual_at_overlap,
        F.col(IndCqc.non_res_with_dormancy_model)
        - F.col(TempColumns.non_res_without_dormancy_model_adjusted),
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
        F.col(TempColumns.non_res_without_dormancy_model_adjusted).isNotNull()
        & F.col(TempColumns.residual_at_overlap).isNotNull()
    )

    df = df.withColumn(
        TempColumns.non_res_without_dormancy_model_adjusted_and_residual_applied,
        F.when(
            model_and_residual_are_not_null,
            F.col(TempColumns.non_res_without_dormancy_model_adjusted)
            + F.col(TempColumns.residual_at_overlap),
        ).otherwise(F.col(TempColumns.non_res_without_dormancy_model_adjusted)),
    )
    return df


def combine_model_predictions(df: DataFrame) -> DataFrame:
    """
    Coalesces the models in the order provided into a single column called 'prediction'.

    The 'non_res_with_dormancy_model' values are used when they are not null.
    If they are null, the 'non_res_without_dormancy_model_adjusted_and_residual_applied' are used.
    If both are null, the prediction will be null.

    Args:
        df (DataFrame): DataFrame with model predictions.

    Returns:
        DataFrame: DataFrame with combined model predictions.
    """
    df = df.withColumn(
        IndCqc.prediction,
        F.coalesce(
            IndCqc.non_res_with_dormancy_model,
            TempColumns.non_res_without_dormancy_model_adjusted_and_residual_applied,
        ),
    )

    return df
