import polars as pl

from projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.utils import (
    join_model_predictions,
)
from projects._03_independent_cqc.utils.utils.utils import get_selected_value

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
    not_null_condition = (
        pl.col(IndCqc.non_res_with_dormancy_model).is_not_null()
        & pl.col(IndCqc.non_res_without_dormancy_model).is_not_null()
    )

    ratio_lf = (
        lf.filter(not_null_condition)
        .group_by(
            IndCqc.related_location, TempColumns.time_registered_banded_and_capped
        )
        .agg(
            pl.mean(IndCqc.non_res_with_dormancy_model).alias(
                TempColumns.avg_with_dormancy
            ),
            pl.mean(IndCqc.non_res_without_dormancy_model).alias(
                TempColumns.avg_without_dormancy
            ),
        )
    )

    ratio_lf = ratio_lf.with_columns(
        pl.when(pl.col(TempColumns.avg_without_dormancy) != 0)
        .then(
            pl.col(TempColumns.avg_with_dormancy).truediv(
                pl.col(TempColumns.avg_without_dormancy)
            )
        )
        .otherwise(1.0)
        .alias(TempColumns.adjustment_ratio)
    )

    lf = lf.join(
        ratio_lf,
        [IndCqc.related_location, TempColumns.time_registered_banded_and_capped],
        "left",
    )

    lf = lf.with_columns(
        pl.col(IndCqc.non_res_without_dormancy_model)
        .mul(pl.col(TempColumns.adjustment_ratio))
        .alias(TempColumns.non_res_without_dormancy_model_adjusted)
    )

    return lf


def calculate_and_apply_residuals(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates and applies residuals between models at the first point in time
    when both models exist to smooth predictions.

    Args:
        lf (pl.LazyFrame): LazyFrame with model predictions.

    Returns:
        pl.LazyFrame: LazyFrame with smoothed predictions.
    """
    first_overlap_lf = lf.sort(
        [IndCqc.location_id, IndCqc.cqc_location_import_date]
    ).with_columns(
        pl.col(IndCqc.cqc_location_import_date)
        .filter(pl.col(IndCqc.non_res_with_dormancy_model).is_not_null())
        .first()
        .over(IndCqc.location_id)
        .alias(TempColumns.first_overlap_date)
    )

    residual_lf = calculate_residuals(first_overlap_lf)

    lf = lf.join(residual_lf, IndCqc.location_id, "left")

    lf = apply_residuals(lf)

    return lf


def calculate_residuals(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates residuals between 'with_dormancy' and
    'non_res_without_dormancy_model_adjusted' models at the first overlap date.

    This function filters the LazyFrame at the first point in time
    ('first_overlap_date') when the 'with_dormancy_model' has values, and to
    locations who have both a 'with_dormancy' and 'without_dormancy_adjusted'
    model prediction. This is used elsewhere in the code to smooth out the
    predictions at the point in time when one model switches the other.

    The residual is calculated as 'with_dormancy' values minus
    'without_dormancy_adjusted'. Only the columns 'location_id' and
    'residual_at_overlap' are returned.

    Args:
        lf (pl.LazyFrame): LazyFrame with model predictions.

    Returns:
        pl.LazyFrame: LazyFrame with 'location_id' and 'residual_at_overlap'.
    """
    overlap_date_condition = pl.col(IndCqc.cqc_location_import_date) == pl.col(
        TempColumns.first_overlap_date
    )
    not_null_condition = (
        pl.col(IndCqc.non_res_with_dormancy_model).is_not_null()
        & pl.col(TempColumns.non_res_without_dormancy_model_adjusted).is_not_null()
    )
    filtered_lf = lf.filter(overlap_date_condition & not_null_condition)

    residual_lf = filtered_lf.with_columns(
        pl.col(IndCqc.non_res_with_dormancy_model)
        .sub(pl.col(TempColumns.non_res_without_dormancy_model_adjusted))
        .alias(TempColumns.residual_at_overlap)
    ).select(IndCqc.location_id, TempColumns.residual_at_overlap)

    return residual_lf


def apply_residuals(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Applies the residuals to smooth predictions when both the model value and
    residual are not null.

    Args:
        lf (pl.LazyFrame): LazyFrame with model predictions and residuals.

    Returns:
        pl.LazyFrame: LazyFrame with smoothed predictions.
    """
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
