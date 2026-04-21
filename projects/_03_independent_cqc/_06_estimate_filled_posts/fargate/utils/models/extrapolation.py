from typing import Tuple

import polars as pl
import pyspark.sql.functions as F

from projects._03_independent_cqc.utils.utils.utils import get_selected_value
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


# TODO
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

    Args:
        df (pl.LazyFrame): The input LazyFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the extrapolated values in the 'extrapolation_model' column.
    """
    # window_spec_all_rows, window_spec_lagged = define_window_specs()

    lf = calculate_first_and_final_submission_dates(lf, column_with_null_values)
    lf = extrapolation_forwards(
        lf,
        column_with_null_values,
        model_to_extrapolate_from,
        extrapolation_method,
    )
    lf = extrapolation_backwards(
        lf,
        column_with_null_values,
        model_to_extrapolate_from,
        extrapolation_method,
    )
    lf = combine_extrapolation(lf)
    lf = lf.drop(IndCqc.first_submission_time, IndCqc.final_submission_time)

    return lf


# def define_window_specs() -> Tuple[Window, Window]:
#     """
#     Defines two window specifications, partitioned by 'location_id' and ordered by 'cqc_location_import_date'.

#     The first window specification ('window_spec_all_rows') includes all rows in the partition.
#     The second window specification ('window_spec_lagged') includes all rows from the start of the partition up to the
#     current row, excluding the current row.

#     Returns:
#         Tuple[Window, Window]: A tuple containing the two window specifications.
#     """
#     window_spec = Window.partitionBy(IndCqc.location_id).orderBy(IndCqc.cqc_location_import_date)

#     window_spec_all_rows = window_spec.rowsBetween(
#         Window.unboundedPreceding, Window.unboundedFollowing
#     )
#     window_spec_lagged = window_spec.rowsBetween(Window.unboundedPreceding, -1)

#     return window_spec_all_rows, window_spec_lagged


def calculate_first_and_final_submission_dates(
    lf: pl.LazyFrame, column_with_null_values: str
) -> pl.LazyFrame:
    """
    Calculates the first and final submission dates based on the '<column_with_null_values>' column.

    Calculates the first and final submission dates based on the '<column_with_null_values>' column
    and adds them as new columns 'first_submission_time' and 'final_submission_time'.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        column_with_null_values (str): The name of the column with null values in.

    Returns:
        pl.LazyFrame: The LazyFrame with the added 'first_submission_time' and 'final_submission_time' columns.
    """
    first_submission_expr = (
        pl.col(IndCqc.cqc_location_import_date)
        .min()
        .alias(IndCqc.first_submission_time)
    )
    final_submission_expr = (
        pl.col(IndCqc.cqc_location_import_date)
        .max()
        .alias(IndCqc.final_submission_time)
    )

    dates_needed = [first_submission_expr, final_submission_expr]

    for date_expr in dates_needed:
        date_expr_lf = (
            lf.drop_nulls(column_with_null_values)
            .sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
            .group_by(IndCqc.location_id)
            .agg(date_expr)
        )

        lf = lf.join(
            date_expr_lf,
            on=IndCqc.location_id,
            how="left",
        )
    return lf


def extrapolation_forwards(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolation_method: str,
) -> pl.LazyFrame:
    """
    Calculates the forward extrapolation and adds it as a new column 'extrapolation_forwards'.

    This function fills null values occurring after the last known value by extrapolating forwards.

    The ratio method is based on multiplying the rate of change from the modelled value at the time
    of the last known non-null value and the modelled value after that point in time.

    The nominal method is based on adding/subtracting the difference between those two modelled values.

    Args:
        lf (pl.LazyFrame): A LazyFrame with a column to extrapolate forwards.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: A LazyFrame with a new column containing forward extrapolated values.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
    """
    ### ADD RANK COLUMN ###
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(IndCqc.cqc_location_import_date).rank().over(IndCqc.location_id))
        .otherwise(None)
        .alias("rank")
    )

    ### POPULATE previous_non_null_value ###
    previous_non_null_lf = (
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .group_by(IndCqc.location_id)
        .agg(
            pl.col(column_with_null_values)
            .min_by("rank")
            .alias(IndCqc.previous_non_null_value)
        )
    )

    lf = lf.join(
        previous_non_null_lf,
        on=IndCqc.location_id,
        how="left",
    )

    ### POPULATE previous_model_value ###
    previous_model_lf = (
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .group_by(IndCqc.location_id)
        .agg(
            pl.col(model_to_extrapolate_from)
            .min_by("rank")
            .alias(IndCqc.previous_model_value)
        )
    )

    lf = lf.join(
        previous_model_lf,
        on=IndCqc.location_id,
        how="left",
    )

    ### CALCULATE EXTRAPOLATION ###

    ratio_expr = (
        pl.col(IndCqc.previous_non_null_value)
        .mul(pl.col(model_to_extrapolate_from))
        .truediv(pl.col(IndCqc.previous_model_value))
    )
    nominal_expr = (
        pl.col(IndCqc.previous_non_null_value)
        .add(pl.col(model_to_extrapolate_from))
        .sub(pl.col(IndCqc.previous_model_value))
    )

    if extrapolation_method == "ratio":
        lf = lf.with_columns(ratio_expr.alias(IndCqc.extrapolation_forwards))

    elif extrapolation_method == "nominal":
        lf = lf.with_columns(nominal_expr.alias(IndCqc.extrapolation_forwards))

    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

    lf = lf.drop("rank", IndCqc.previous_non_null_value, IndCqc.previous_model_value)

    return lf


def extrapolation_backwards(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolation_method: str,
) -> pl.LazyFrame:
    """
    Calculates the backward extrapolation and adds it as a new column 'extrapolation_backwards'.

    This function fills null values occurring before the first known value by extrapolating backwards.

    The ratio method is based on multiplying the rate of change from the modelled value before the
    first known non-null value and the modelled value at the first known non-null timestamp.

    The nominal method is based on adding/subtracting the difference between those two modelled values.

    Args:
        df (pl.LazyFrame): The input LazyFrame.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The name of the column representing the model to extrapolate from.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the added 'extrapolation_backwards' column.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
    """
    ### ADD RANK COLUMN ###
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(IndCqc.cqc_location_import_date).rank().over(IndCqc.location_id))
        .otherwise(None)
        .alias("rank")
    )

    ### POPULATE first_non_null_value ###
    first_non_null_lf = (
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .group_by(IndCqc.location_id)
        .agg(
            pl.col(column_with_null_values)
            .max_by("rank")
            .alias(IndCqc.first_non_null_value)
        )
    )

    lf = lf.join(
        first_non_null_lf,
        on=IndCqc.location_id,
        how="left",
    )

    ### POPULATE first_model_value ###
    first_model_lf = (
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .group_by(IndCqc.location_id)
        .agg(
            pl.col(model_to_extrapolate_from)
            .max_by("rank")
            .alias(IndCqc.first_model_value)
        )
    )

    lf = lf.join(
        first_model_lf,
        on=IndCqc.location_id,
        how="left",
    )

    ### CALCULATE EXTRAPOLATION ###

    ratio_expr = pl.when(
        pl.col(IndCqc.cqc_location_import_date) < pl.col(IndCqc.first_submission_time)
    ).then(
        pl.col(IndCqc.first_non_null_value)
        .mul(pl.col(model_to_extrapolate_from))
        .truediv(pl.col(IndCqc.first_model_value))
    )

    nominal_expr = pl.when(
        pl.col(IndCqc.cqc_location_import_date) < pl.col(IndCqc.first_submission_time)
    ).then(
        pl.col(IndCqc.first_non_null_value)
        .add(pl.col(model_to_extrapolate_from))
        .sub(pl.col(IndCqc.first_model_value))
    )

    if extrapolation_method == "ratio":
        lf = lf.with_columns(ratio_expr.alias(IndCqc.extrapolation_backwards))

    elif extrapolation_method == "nominal":
        lf = lf.with_columns(nominal_expr.alias(IndCqc.extrapolation_backwards))

    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")
    lf.show()
    lf = lf.drop("rank", IndCqc.first_non_null_value, IndCqc.first_model_value)

    return lf


def combine_extrapolation(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Combines forward and backward extrapolation values into a single column based on the specified model.

    This function creates a new column named 'extrapolation_model' which contains:
    - Forward extrapolation values if 'cqc_location_import_date' is greater than the 'final_submission_time'.
    - Backward extrapolation values if 'cqc_location_import_date' is less than the 'first_submission_time'.

    Args:
        df (pl.LazyFrame): The input LazyFrame containing the columns 'cqc_location_import_date', 'first_submission_time',
            'final_submission_time', 'extrapolation_forwards', and 'extrapolation_backwards'.

    Returns:
        pl.LazyFrame: The LazyFrame with the added combined extrapolation column.
    """
    combine_extrapolation_expr = (
        pl.when(
            pl.col(IndCqc.cqc_location_import_date)
            > pl.col(IndCqc.final_submission_time)
        )
        .then(pl.col(IndCqc.extrapolation_forwards))
        .when(
            pl.col(IndCqc.cqc_location_import_date)
            < pl.col(IndCqc.first_submission_time)
        )
        .then(pl.col(IndCqc.extrapolation_backwards))
    )
    lf = lf.with_columns(combine_extrapolation_expr.alias(IndCqc.extrapolation_model))
    return lf
