import polars as pl

from projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.utils import (
    join_model_predictions,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_names.ind_cqc_pipeline_columns import (
    NonResWithAndWithoutDormancyCombinedColumns as TempColumns,
)
from utils.column_values.categorical_column_values import CareHome

non_res_columns_to_select = [
    IndCqc.location_id,
    IndCqc.cqc_location_import_date,
    IndCqc.care_home,
    IndCqc.related_location,
    IndCqc.time_registered,
    IndCqc.non_res_without_dormancy_model,
    IndCqc.non_res_with_dormancy_model,
]


def combine_non_res_with_and_without_dormancy_models(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Creates a combined model prediction by adjusting the 'without_dormancy'
    model to align with the 'with_dormancy' model and applying residual
    corrections for smoothing.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing 'without_dormancy' and
            'with_dormancy' model predictions.

    Returns:
        pl.LazyFrame: The original LazyFrame with the combined model predictions
            joined in.
    """
    non_res_locations_lf = lf.select(non_res_columns_to_select).filter(
        pl.col(IndCqc.care_home) == CareHome.not_care_home
    )

    combined_models_lf = group_time_registered_to_six_month_bands(non_res_locations_lf)

    combined_models_lf = calculate_and_apply_model_ratios(combined_models_lf)

    combined_models_lf = calculate_and_apply_residuals(combined_models_lf)

    combined_models_lf = combined_models_lf.with_columns(
        pl.coalesce(
            IndCqc.non_res_with_dormancy_model,
            TempColumns.non_res_without_dormancy_model_adjusted_and_residual_applied,
        ).alias(IndCqc.prediction)
    )

    locations_with_predictions_df = join_model_predictions(
        lf,
        combined_models_lf,
        IndCqc.non_res_combined_model,
        include_run_id=False,
    )

    return locations_with_predictions_df


def group_time_registered_to_six_month_bands(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Groups 'time_registered' into six-month bands and caps values beyond ten
    years.

    The 'time_registered' column starts at one for individuals registered for up
    to one month. To align with six-month bands where '1' represents up to six
    months, we subtract one before dividing by six.

    Since registration patterns change little beyond ten years and sample sizes
    decrease, we cap values at ten years (equivalent to 20 six-month bands) for
    consistency.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the 'time_registered'
            column.

    Returns:
        pl.LazyFrame: LazyFrame with 'time_registered' transformed into
            six-month bands, capped at ten years.
    """
    six_month_time_bands: int = 6
    ten_years: int = 20  # 10 years is 20 six-month bands

    lf = lf.with_columns(
        pl.col(IndCqc.time_registered)
        .sub(pl.lit(1))
        .truediv(pl.lit(six_month_time_bands))
        .floor()
        .clip(upper_bound=pl.lit(ten_years))
        .alias(TempColumns.time_registered_banded_and_capped)
    )
    return lf


def calculate_and_apply_model_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates the ratio between 'with_dormancy' and 'without_dormancy' models
    by partitioning the dataset based on 'related_location' and
    'time_registered_banded_and_capped'.

    Args:
        lf (pl.LazyFrame): Input LazyFrame with model predictions.

    Returns:
        pl.LazyFrame: LazyFrame with partition-level ratios applied.
    """
    group_cols = [
        IndCqc.related_location,
        TempColumns.time_registered_banded_and_capped,
    ]

    not_null_condition = (
        pl.col(IndCqc.non_res_with_dormancy_model).is_not_null()
        & pl.col(IndCqc.non_res_without_dormancy_model).is_not_null()
    )

    avg_with_expr = (
        pl.when(not_null_condition)
        .then(pl.col(IndCqc.non_res_with_dormancy_model))
        .otherwise(None)
        .mean()
        .over(group_cols)
    )

    avg_without_expr = (
        pl.when(not_null_condition)
        .then(pl.col(IndCqc.non_res_without_dormancy_model))
        .otherwise(None)
        .mean()
        .over(group_cols)
    )

    adjustment_ratio_expr = (
        pl.when(avg_without_expr.is_null())
        .then(None)
        .when(avg_without_expr != 0)
        .then(avg_with_expr.truediv(avg_without_expr))
        .otherwise(1.0)
    )

    return lf.with_columns(
        avg_with_expr.alias(TempColumns.avg_with_dormancy),
        avg_without_expr.alias(TempColumns.avg_without_dormancy),
        adjustment_ratio_expr.alias(TempColumns.adjustment_ratio),
        (
            pl.col(IndCqc.non_res_without_dormancy_model).mul(adjustment_ratio_expr)
        ).alias(TempColumns.non_res_without_dormancy_model_adjusted),
    )


def calculate_and_apply_residuals(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates and applies residuals between models at the first point in time
    when both models exist to smooth predictions.

    Args:
        lf (pl.LazyFrame): LazyFrame with model predictions.

    Returns:
        pl.LazyFrame: LazyFrame with smoothed predictions.
    """
    first_non_res_with_dormancy = (
        pl.col(IndCqc.non_res_with_dormancy_model)
        .filter(pl.col(IndCqc.non_res_with_dormancy_model).is_not_null())
        .first()
        .over(
            partition_by=IndCqc.location_id,
            order_by=IndCqc.cqc_location_import_date,
        )
    )
    first_non_res_without_dormancy_model_adjusted = (
        pl.col(TempColumns.non_res_without_dormancy_model_adjusted)
        .filter(pl.col(IndCqc.non_res_with_dormancy_model).is_not_null())
        .first()
        .over(
            partition_by=IndCqc.location_id,
            order_by=IndCqc.cqc_location_import_date,
        )
    )

    lf = lf.with_columns(
        first_non_res_with_dormancy.sub(
            first_non_res_without_dormancy_model_adjusted
        ).alias(TempColumns.residual_at_overlap)
    )

    model_and_residual_are_not_null_condition = (
        pl.col(TempColumns.non_res_without_dormancy_model_adjusted).is_not_null()
        & pl.col(TempColumns.residual_at_overlap).is_not_null()
    )

    lf = lf.with_columns(
        pl.when(model_and_residual_are_not_null_condition)
        .then(
            pl.col(TempColumns.non_res_without_dormancy_model_adjusted).add(
                pl.col(TempColumns.residual_at_overlap)
            )
        )
        .otherwise(pl.col(TempColumns.non_res_without_dormancy_model_adjusted))
        .alias(TempColumns.non_res_without_dormancy_model_adjusted_and_residual_applied)
    )

    return lf
