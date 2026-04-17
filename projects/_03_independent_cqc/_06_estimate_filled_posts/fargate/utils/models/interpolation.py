from typing import Optional

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_interpolation(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    method: str,
    new_column_name: Optional[str] = IndCqc.interpolation_model,
    max_days_between_submissions: Optional[int] = None,
) -> pl.LazyFrame:
    """
    Perform interpolation on a column with null values and adds as a new column
    called 'interpolation_model'.

    This function can produce two styles of interpolation:
        - straight line interpolation
        - trend line interpolation (as part of imputation model), where it uses
            the extrapolation_forwards values as a trend line to guide
            interpolated predictions.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        column_with_null_values (str): The name of the column that contains null
            values to be interpolated.
        method (str): The choice of method. Must be either 'straight' or 'trend'
        new_column_name (Optional[str]): The name of the new column. Default is
            'interpolation_model'
        max_days_between_submissions (Optional[int]): Maximum allowed days between
            submissions to apply interpolation. If None, interpolation is
            applied to all rows.

    Returns:
        pl.LazyFrame: The LazyFrame with the interpolated values in the
            'interpolation_model' column.

    Raises:
        ValueError: If chosen method does not match 'straight' or 'trend'.
    """
    lf = calculate_proportion_of_time_between_submissions(lf, column_with_null_values)

    if method == "trend":
        lf = calculate_residuals(
            lf,
            column_with_null_values,
            IndCqc.extrapolation_forwards,
        )
        lf = calculate_interpolated_values(
            lf, IndCqc.extrapolation_forwards, new_column_name
        )

    elif method == "straight":
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        lf = lf.with_columns(
            (
                pl.when(pl.col(column_with_null_values).is_not_null())
                .then(pl.col(column_with_null_values))
                .otherwise(None)
                .shift(1)
                .forward_fill()
                .over(IndCqc.location_id)
            ).alias(IndCqc.previous_non_null_value)
        )

        lf = calculate_residuals(
            lf,
            column_with_null_values,
            IndCqc.previous_non_null_value,
        )
        lf = calculate_interpolated_values(
            lf,
            IndCqc.previous_non_null_value,
            new_column_name,
            max_days_between_submissions,
        )
        lf = lf.drop(IndCqc.previous_non_null_value)

    else:
        raise ValueError("Error: method must be either 'straight' or 'trend'")

    lf = lf.drop(
        IndCqc.time_between_submissions,
        IndCqc.proportion_of_time_between_submissions,
        IndCqc.residual,
    )

    return lf


def calculate_residuals(
    lf: pl.LazyFrame, first_column: str, second_column: str
) -> pl.LazyFrame:
    """
    Calculate the residual between two non-null values (first_column minus
    second_column).

    This function computes the residuals between two non-null values in the
    specified columns. It creates a temporary column to store the difference
    between the non-null values, then duplicates the first non-null residual
    over a specified window and assigns it to a new column called 'residual'.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        first_column (str): The name of the first column that contains values.
        second_column (str): The name of the second column that contains values.

    Returns:
        pl.LazyFrame: The LazyFrame with the calculated residuals in a new
            column.
    """
    lf = lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])

    residual_expr = (
        pl.when(
            pl.col(first_column).is_not_null() & pl.col(second_column).is_not_null()
        )
        .then(pl.col(first_column) - pl.col(second_column))
        .otherwise(None)
    )

    lf = lf.with_columns(
        pl.when(pl.col(second_column).is_not_null())
        .then(residual_expr.backward_fill().over(IndCqc.location_id))
        .alias(IndCqc.residual)
    )
    return lf


def calculate_proportion_of_time_between_submissions(
    lf: pl.LazyFrame, column_with_null_values: str
) -> pl.LazyFrame:
    """
    Calculates the proportion of time, based on cqc_location_import_date of
    each row, between two non-null submission times.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        column_with_null_values (str): The name of the column that contains
            null values.

    Returns:
        pl.LazyFrame: The LazyFrame with the new column added.
    """
    lf = (
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .with_columns(
            # Create conditional column: cqc_location_import_date where column_with_null_values is not null
            pl.when(pl.col(column_with_null_values).is_not_null())
            .then(pl.col(IndCqc.cqc_location_import_date))
            .alias("_temp_time")
        )
        .with_columns(
            # Previous: forward fill within partition (last non-null up to current row)
            pl.col("_temp_time")
            .forward_fill()
            .over(IndCqc.location_id)
            .alias(IndCqc.previous_submission_time),
            # Next: backward fill within partition (first non-null from current row onward)
            pl.col("_temp_time")
            .backward_fill()
            .over(IndCqc.location_id)
            .alias(IndCqc.next_submission_time),
        )
        .drop("_temp_time")
    )

    current_import_date_is_between_known_submissions = (
        pl.col(IndCqc.previous_submission_time)
        < pl.col(IndCqc.cqc_location_import_date)
    ) & (pl.col(IndCqc.next_submission_time) > pl.col(IndCqc.cqc_location_import_date))

    lf = lf.with_columns(
        pl.when(current_import_date_is_between_known_submissions)
        .then(
            (
                pl.col(IndCqc.next_submission_time)
                - pl.col(IndCqc.previous_submission_time)
            ).dt.total_days()
        )
        .alias(IndCqc.time_between_submissions)
    )

    lf = lf.with_columns(
        pl.when(current_import_date_is_between_known_submissions)
        .then(
            (
                pl.col(IndCqc.cqc_location_import_date)
                - pl.col(IndCqc.previous_submission_time)
            ).dt.total_days()
            / pl.col(IndCqc.time_between_submissions)
        )
        .alias(IndCqc.proportion_of_time_between_submissions)
    ).drop(IndCqc.previous_submission_time, IndCqc.next_submission_time)

    return lf


def calculate_interpolated_values(
    lf: pl.LazyFrame,
    column_to_interpolate_from: str,
    new_column_name: str,
    max_days_between_submissions: Optional[int] = None,
) -> pl.LazyFrame:
    """
    Calculate interpolated values for a new column in a LazyFrame, optionally
    constrained by a max time between submissions.

    This function takes a LazyFrame and interpolates values from an existing
    column to create a new column. The interpolation is based on the residual
    and the proportion of time between submissions.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        column_to_interpolate_from (str): The name of the column from which to
            interpolate values.
        new_column_name (str): The name of the new column to be created with
            interpolated values.
        max_days_between_submissions (Optional[int]): Maximum allowed days between
            submissions to apply interpolation. If None, interpolation is applied
            to all rows.

    Returns:
        pl.LazyFrame: A new LazyFrame with the interpolated values added as a
            new column.
    """
    if max_days_between_submissions is not None:
        condition_is_true = pl.col(IndCqc.time_between_submissions) <= pl.lit(
            max_days_between_submissions
        )
    else:
        condition_is_true = pl.lit(True)

    interpolated_value = pl.col(column_to_interpolate_from) + (
        pl.col(IndCqc.residual) * pl.col(IndCqc.proportion_of_time_between_submissions)
    )

    lf = lf.with_columns(
        pl.when(condition_is_true)
        .then(interpolated_value)
        .otherwise(None)
        .alias(new_column_name)
    )
    return lf
