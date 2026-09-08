from typing import List, Optional

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
    group_columns: Optional[List[str]] = None,
) -> pl.LazyFrame:
    """
    Perform extrapolation on a column with null values using specified models.

    Values before the first known submission in 'column_with_null_values' and
    after the last known submission are extrapolated, either by nominal or ratio
    method as specified in 'extrapolation_method'.

    The process consists of: - Computing per-group aggregates such
    as first/last submission dates and first submitted values
    - Deriving previous submitted values within each group
    - Applying either ratio-based or nominal extrapolation logic
    - Producing two output columns:
        - `extrapolation_forwards`: values extrapolated after the first
          observation
        - `extrapolation_model`: combined forward and backward extrapolated
          values

    The 'extrapolation_forwards' column is required for the
    'interpolation_model' computation and applies forward extrapolation to all
    values after the first submission.

    Extrapolation is applied as follows:
        - Forward extrapolation: for dates after the last submitted value
        - Backward extrapolation: for dates before the first observed value

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing time series data.
        column_with_null_values (str): Column containing observed values with
            nulls.
        model_to_extrapolate_from (str): Column providing the model values used
            to guide extrapolation.
        extrapolation_method (str): Method used for extrapolation, either:
            - "ratio": scales values based on proportional change in the model
            - "nominal": adjusts values based on absolute change in the model
        group_columns (Optional[List[str]]): Columns to partition the
            per-group calculations by. Defaults to `[location_id]` alone
            when None. A caller should widen this when a location's own
            attributes can change over time (e.g. `care_home` status), so
            that periods with different attribute values don't get mixed
            into the same extrapolation group.

    Returns:
        pl.LazyFrame: LazyFrame with extrapolated columns added and temporary
            columns removed.

    Raises:
        ValueError: If `extrapolation_method` is not "ratio" or "nominal".
    """
    group_columns = group_columns or [IndCqc.location_id]

    lf = build_extrapolation_aggregates(
        lf, column_with_null_values, model_to_extrapolate_from, group_columns
    )

    # Only keep model values when we actually have an observation
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(model_to_extrapolate_from))
        .alias(TEMP.model_with_nulls)
    )

    # Get last observed values
    lf = lf.with_columns(
        [
            get_previous_value(column_with_null_values, group_columns).alias(
                TEMP.previous_value
            ),
            get_previous_value(TEMP.model_with_nulls, group_columns).alias(
                TEMP.previous_model
            ),
        ]
    )

    expr = ExtrapolationCalculationExpressions(model_to_extrapolate_from)

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
    lf: pl.LazyFrame,
    value_col: str,
    model_col: str,
    group_columns: Optional[List[str]] = None,
) -> pl.LazyFrame:
    """
    Add per-group extrapolation boundary values as columns on every row.

    For each group (`location_id` alone by default), computes the first and
    last submission dates where `value_col` is non-null, and the
    `value_col`/`model_col` values observed on that first submission date.
    These are added as new columns rather than aggregated into a separate
    LazyFrame, so no join is needed to bring them back onto the full dataset —
    cheaper than the group_by+join equivalent since it avoids materialising
    and merging a second frame.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing time series data.
        value_col (str): Column containing observed values with nulls.
        model_col (str): Column containing model values used for extrapolation.
        group_columns (Optional[List[str]]): Columns to partition the
            calculations by. Defaults to `[location_id]` alone when None.

    Returns:
        pl.LazyFrame: The input LazyFrame with four extra columns —
            `first_submission_time`, `final_submission_time`, `first_value`, and
            `first_model` — repeated across every row for a given group.
    """
    group_columns = group_columns or [IndCqc.location_id]

    is_observed = pl.col(value_col).is_not_null()
    observed_date = pl.when(is_observed).then(pl.col(IMPORT_DATE))

    lf = lf.with_columns(
        observed_date.min().over(group_columns).alias(TEMP.first_submission_time),
        observed_date.max().over(group_columns).alias(TEMP.final_submission_time),
    )

    is_first_observed_row = is_observed & (
        pl.col(IMPORT_DATE) == pl.col(TEMP.first_submission_time)
    )

    return lf.with_columns(
        pl.when(is_first_observed_row)
        .then(pl.col(value_col))
        .max()
        .over(group_columns)
        .alias(TEMP.first_value),
        pl.when(is_first_observed_row)
        .then(pl.col(model_col))
        .max()
        .over(group_columns)
        .alias(TEMP.first_model),
    )


def get_previous_value(col: str, group_columns: Optional[List[str]] = None) -> pl.Expr:
    """
    Generate an expression for the previous observed non-null value within a group.

    This expression forward-fills null values within each group, then shifts
    the result by one row to obtain the most recent prior observed value.

    It is used to support forward extrapolation, where the last known value
    prior to a gap is required.

    Args:
        col (str): Name of the column to compute previous values for.
        group_columns (Optional[List[str]]): Columns to partition the
            calculation by. Defaults to `[location_id]` alone when None.

    Returns:
        pl.Expr: Polars expression representing the previous observed value
        within each group.
    """
    group_columns = group_columns or [IndCqc.location_id]

    return (
        pl.col(col)
        .sort_by(IMPORT_DATE)
        .fill_null(strategy="forward")
        .shift(1)
        .over(group_columns, order_by=IMPORT_DATE)
    )


class ExtrapolationCalculationExpressions:
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
