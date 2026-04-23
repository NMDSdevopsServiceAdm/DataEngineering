import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import ExtrapolationColumns as TempCol
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_extrapolation(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolation_method: str,
) -> pl.LazyFrame:
    """
    Perform extrapolation on a column with null values using specified models.

    This function calculates the first and final submission dates, performs forward and backward extrapolation,
    and combines the extrapolated results into a new column.

    Values before the first known submission in 'column_with_null_values' and after the last known submission are
    extrapolated, either by nominal or ratio method as specified in 'extrapolation_method'.

    The ratio method is based on multiplying the known value by the rate of change of the '<model_column_name>'.

    The nominal method is based on adding/subtracting the nominal change of the '<model_column_name>' to the known value.

    The function adds two columns: 'extrapolation_forwards' and 'extrapolation_model'. The interpolation model uses
    'extrapolation_forwards' in its computations.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the extrapolated values in the 'extrapolation_model' and 'extrapolation_forwards' columns.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
    """

    agg_lf = (
        lf.filter(pl.col(column_with_null_values).is_not_null())
        .group_by(IndCqc.location_id)
        .agg(
            [
                # First / last submission dates
                pl.col(IndCqc.cqc_location_import_date)
                .min()
                .alias(TempCol.first_submission_time),
                pl.col(IndCqc.cqc_location_import_date)
                .max()
                .alias(TempCol.final_submission_time),
                # First known values (for backwards extrapolation)
                pl.col(column_with_null_values)
                .sort_by(IndCqc.cqc_location_import_date)
                .first()
                .alias(TempCol.first_non_null_value),
                pl.col(model_to_extrapolate_from)
                .sort_by(IndCqc.cqc_location_import_date)
                .first()
                .alias(TempCol.first_model_value),
            ]
        )
    )

    lf = lf.join(agg_lf, on=IndCqc.location_id, how="left")

    # Only keep model values when we actually have an observation
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(model_to_extrapolate_from))
        .alias(TempCol.model_with_null_values)
    )

    # Get last observed values
    lf = lf.with_columns(
        [
            pl.col(column_with_null_values)
            .fill_null(strategy="forward")
            .shift(1)
            .over(IndCqc.location_id)
            .alias(TempCol.previous_non_null_value),
            pl.col(TempCol.model_with_null_values)
            .fill_null(strategy="forward")
            .shift(1)
            .over(IndCqc.location_id)
            .alias(TempCol.previous_model_value),
        ]
    )

    expr = ExtrapolationExpressions(model_to_extrapolate_from)

    if extrapolation_method == "ratio":
        forward_expr = expr.forward_ratio
        backward_expr = expr.backward_ratio
    elif extrapolation_method == "nominal":
        forward_expr = expr.forward_nominal
        backward_expr = expr.backward_nominal
    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

    lf = lf.with_columns(
        [
            # Forward extrapolation (after last known)
            pl.when(
                pl.col(IndCqc.cqc_location_import_date)
                > pl.col(TempCol.first_submission_time)
            )
            .then(forward_expr)
            .alias(IndCqc.extrapolation_forwards),
            # Combined final column
            pl.when(
                pl.col(IndCqc.cqc_location_import_date)
                > pl.col(TempCol.final_submission_time)
            )
            .then(forward_expr)
            .when(
                pl.col(IndCqc.cqc_location_import_date)
                < pl.col(TempCol.first_submission_time)
            )
            .then(backward_expr)
            .alias(IndCqc.extrapolation_model),
        ]
    )

    columns_to_drop = list(vars(TempCol()).values())
    lf = lf.drop(*columns_to_drop)

    return lf


class ExtrapolationExpressions:
    """Define polars logic for extrapolation values and concepts

    This class creates various polars expressions required for computing extrapolation.

    Args:
        model_to_extrapolate_from (str): The model used for extrapolation.
    """

    def __init__(self, model_to_extrapolate_from: str):

        self.forward_ratio = (
            pl.col(TempCol.previous_non_null_value) * pl.col(model_to_extrapolate_from)
        ) / pl.col(TempCol.previous_model_value)

        self.backward_ratio = (
            pl.col(TempCol.first_non_null_value) * pl.col(model_to_extrapolate_from)
        ) / pl.col(TempCol.first_model_value)

        self.forward_nominal = (
            pl.col(TempCol.previous_non_null_value)
            + pl.col(model_to_extrapolate_from)
            - pl.col(TempCol.previous_model_value)
        )

        self.backward_nominal = (
            pl.col(TempCol.first_non_null_value)
            + pl.col(model_to_extrapolate_from)
            - pl.col(TempCol.first_model_value)
        )
