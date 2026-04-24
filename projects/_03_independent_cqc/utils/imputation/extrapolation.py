import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import ExtrapolationColumns
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

TEMP = ExtrapolationColumns()  # Temporary column names used during extrapolation
IMPORT_DATE: str = IndCqc.cqc_location_import_date


def model_extrapolation(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolation_method: str,
) -> pl.LazyFrame:
    """
    Extrapolate missing values in a column using a reference model.

    This function performs forward and backward extrapolation for values that
    fall outside the observed range of `column_with_null_values`, using a
    specified model column and extrapolation method.

    The process consists of:
    - Computing per-group (location_id) aggregates such as first/last submission
      dates and first observed values
    - Deriving previous observed values within each group
    - Applying either ratio-based or nominal extrapolation logic
    - Producing two output columns:
        - `extrapolation_forwards`: values extrapolated after the first observation
        - `extrapolation_model`: combined forward and backward extrapolated values

    Extrapolation is applied as follows:
    - Forward extrapolation: for dates after the last observed value
    - Backward extrapolation: for dates before the first observed value

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing time series data.
        column_with_null_values (str): Column containing observed values with nulls.
        model_to_extrapolate_from (str): Column providing the model values used
            to guide extrapolation.
        extrapolation_method (str): Method used for extrapolation. Must be one of:
            - "ratio": scales values based on proportional change in the model
            - "nominal": adjusts values based on absolute change in the model

    Returns:
        pl.LazyFrame: LazyFrame with extrapolated columns added and temporary
            columns removed.

    Raises:
        ValueError: If `extrapolation_method` is not "ratio" or "nominal".
    """
    agg_lf = build_extrapolation_aggregates(
        lf, column_with_null_values, model_to_extrapolate_from
    )

    lf = lf.join(agg_lf, on=IndCqc.location_id, how="left")

    # Only keep model values when we actually have an observation
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(model_to_extrapolate_from))
        .alias(TEMP.model_with_nulls)
    )

    # Get last observed values
    lf = lf.with_columns(
        [
            get_previous_value(column_with_null_values).alias(TEMP.previous_value),
            get_previous_value(TEMP.model_with_nulls).alias(TEMP.previous_model),
        ]
    )

    expr = ExtrapolationExpressions(model_to_extrapolate_from)

    method_map = {
        "ratio": (expr.forward_ratio, expr.backward_ratio),
        "nominal": (expr.forward_nominal, expr.backward_nominal),
    }
    try:
        forward_expr, backward_expr = method_map[extrapolation_method]
    except KeyError:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

    lf = lf.with_columns(
        [
            # Forward extrapolation (after first known)
            pl.when(pl.col(IMPORT_DATE) > pl.col(TEMP.first_submission_time))
            .then(forward_expr)
            .alias(IndCqc.extrapolation_forwards),
            # Combined final column
            pl.when(pl.col(IMPORT_DATE) > pl.col(TEMP.final_submission_time))
            .then(forward_expr)
            .when(pl.col(IMPORT_DATE) < pl.col(TEMP.first_submission_time))
            .then(backward_expr)
            .alias(IndCqc.extrapolation_model),
        ]
    )

    cols_to_drop = list(vars(TEMP).values())
    lf = lf.drop(*cols_to_drop)

    return lf


def build_extrapolation_aggregates(
    lf: pl.LazyFrame, value_col: str, model_col: str
) -> pl.LazyFrame:
    """
    Compute per-group aggregates required for extrapolation.

    This function filters to rows where `value_col` is non-null and calculates,
    for each `location_id`:
        - The first and last submission timestamps
        - The first observed value of `value_col`
        - The corresponding first value of the model column

    These aggregates are later joined back to the original dataset to support
    backward extrapolation and boundary detection.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing time series data.
        value_col (str): Column containing observed values with nulls.
        model_col (str): Column containing model values used for extrapolation.

    Returns:
        pl.LazyFrame: Aggregated LazyFrame with one row per `location_id`,
        containing the required extrapolation metadata.
    """
    return (
        lf.filter(pl.col(value_col).is_not_null())
        .group_by(IndCqc.location_id)
        .agg(
            [
                pl.col(IMPORT_DATE).min().alias(TEMP.first_submission_time),
                pl.col(IMPORT_DATE).max().alias(TEMP.final_submission_time),
                pl.col(value_col).sort_by(IMPORT_DATE).first().alias(TEMP.first_value),
                pl.col(model_col).sort_by(IMPORT_DATE).first().alias(TEMP.first_model),
            ]
        )
    )


def get_previous_value(col: str) -> pl.Expr:
    """
    Generate an expression for the previous observed non-null value within a group.

    This expression forward-fills null values within each `location_id` group,
    then shifts the result by one row to obtain the most recent prior observed value.

    It is used to support forward extrapolation, where the last known value
    prior to a gap is required.

    Args:
        col (str): Name of the column to compute previous values for.

    Returns:
        pl.Expr: Polars expression representing the previous observed value
        within each `location_id` group.
    """
    return pl.col(col).fill_null(strategy="forward").shift(1).over(IndCqc.location_id)


class ExtrapolationExpressions:
    """
    Collection of Polars expressions for forward and backward extrapolation.

    This class defines reusable expressions for computing extrapolated values
    based on either ratio or nominal change relative to a model column.

    The expressions rely on precomputed helper columns, including:
    - Previous observed values
    - First observed values
    - Corresponding model values

    Two extrapolation strategies are supported:
    - Ratio: scales values proportionally to changes in the model
    - Nominal: adjusts values based on absolute differences in the model

    Attributes:
        forward_ratio (pl.Expr): Forward extrapolation using proportional change.
        backward_ratio (pl.Expr): Backward extrapolation using proportional change.
        forward_nominal (pl.Expr): Forward extrapolation using absolute change.
        backward_nominal (pl.Expr): Backward extrapolation using absolute change.

    Args:
        model_to_extrapolate_from (str): Column providing model values used
            in extrapolation calculations.
    """

    forward_ratio: pl.Expr
    backward_ratio: pl.Expr
    forward_nominal: pl.Expr
    backward_nominal: pl.Expr

    def __init__(self, model_to_extrapolate_from: str):

        self.forward_ratio = (
            pl.col(TEMP.previous_value) * pl.col(model_to_extrapolate_from)
        ) / pl.col(TEMP.previous_model)

        self.backward_ratio = (
            pl.col(TEMP.first_value) * pl.col(model_to_extrapolate_from)
        ) / pl.col(TEMP.first_model)

        self.forward_nominal = (
            pl.col(TEMP.previous_value)
            + pl.col(model_to_extrapolate_from)
            - pl.col(TEMP.previous_model)
        )

        self.backward_nominal = (
            pl.col(TEMP.first_value)
            + pl.col(model_to_extrapolate_from)
            - pl.col(TEMP.first_model)
        )
