import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_interpolation(
    direct_payments_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Performs linear interpolation of missing values for the
    estimated proportion of service users employing staff.

    This function:
    1. Filters rows with known (non-null) values.
    2. Determines the first and last available year per LA_AREA.
    3. Generates a complete year range for each LA_AREA.
    4. Joins known values onto the full year range.
    5. Derives helper columns for interpolation (previous/next values and years).
    6. Computes interpolated values for missing years.
    7. Joins interpolated results back to the original dataset.

    Args:
    direct_payments_lf (pl.LazyFrame): Input LazyFrame with columns LA_AREA,
        YEAR_AS_INTEGER and ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF

    Returns:
    pl.LazyFrame: Original LazyFrame with an additional column
        ESTIMATE_USING_INTERPOLATION
    """
    known_service_users_employing_staff_lf = direct_payments_lf.select(
        [
            DP.LA_AREA,
            DP.YEAR_AS_INTEGER,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        ]
    ).filter(
        pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).is_not_null()
    )

    first_submission_expr = (
        pl.col(DP.YEAR_AS_INTEGER).min().cast(pl.Int32).alias(DP.FIRST_SUBMISSION_YEAR)
    )
    last_submission_expr = (
        pl.col(DP.YEAR_AS_INTEGER).max().cast(pl.Int32).alias(DP.LAST_SUBMISSION_YEAR)
    )
    first_and_last_submission_year_lf = known_service_users_employing_staff_lf.group_by(
        DP.LA_AREA
    ).agg(
        [
            first_submission_expr,
            last_submission_expr,
        ]
    )

    all_dates_lf = (
        first_and_last_submission_year_lf.with_columns(
            pl.int_ranges(
                pl.col(DP.FIRST_SUBMISSION_YEAR),
                pl.col(DP.LAST_SUBMISSION_YEAR) + 1,
            ).alias(DP.INTERPOLATION_YEAR)
        )
        .explode(DP.INTERPOLATION_YEAR)
        .drop([DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR])
    )

    all_dates_lf = all_dates_lf.rename({DP.INTERPOLATION_YEAR: DP.YEAR_AS_INTEGER})

    all_dates_lf = all_dates_lf.join(
        known_service_users_employing_staff_lf, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )

    all_dates_lf = all_dates_lf.with_columns(
        pl.when(
            pl.col(
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ).is_not_null()
        )
        .then(pl.col(DP.YEAR_AS_INTEGER))
        .alias(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED)
    )

    all_dates_lf = interpolate_values_for_all_dates(all_dates_lf)

    direct_payments_lf = direct_payments_lf.join(
        all_dates_lf, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )

    return direct_payments_lf


def interpolate_values_for_all_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Enriches a dataset with previous and next known values and computes
    interpolated estimates for missing observations.

    This function applies a sequence of transformations:
    - Forward fill to obtain previous known values and years.
    - Backward fill to obtain next known values and years.
    - Linear interpolation using surrounding known data points.

    Args:
    lf (pl.LazyFrame): Input LazyFrame with columns LA_AREA, YEAR_AS_INTEGER,
        ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF and
        ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED

    Returns:
    pl.LazyFrame: LazyFrame containing LA_AREA, YEAR_AS_INTEGER and
        ESTIMATE_USING_INTERPOLATION
    """
    lf = get_previous_value_in_column(
        lf,
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF,
    )
    lf = get_previous_value_in_column(
        lf,
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
        DP.PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
    )
    lf = get_next_value_in_new_column(
        lf,
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF,
    )
    lf = get_next_value_in_new_column(
        lf,
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
        DP.NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
    )
    lf = calculated_interpolated_values_in_new_column(
        lf, DP.ESTIMATE_USING_INTERPOLATION
    )
    return lf


def get_previous_value_in_column(
    lf: pl.LazyFrame, column_name: str, new_column_name: str
) -> pl.LazyFrame:
    """
    Computes the last known non-null value of a column up to the current row
    within each LA_AREA, based on ascending year order.

    Args:
    lf (pl.LazyFrame): Input LazyFrame.
    column_name (str): Name of the column to compute previous values for.
    new_column_name (str): Name of the output column to store the result.

    Returns:
    pl.LazyFrame: LazyFrame with an additional column containing the previous
        non-null value.
    """
    return lf.sort([DP.LA_AREA, DP.YEAR_AS_INTEGER]).with_columns(
        pl.col(column_name).forward_fill().over(DP.LA_AREA).alias(new_column_name)
    )


def get_next_value_in_new_column(
    lf: pl.LazyFrame, column_name: str, new_column_name: str
) -> pl.LazyFrame:
    """
    Computes the next known non-null value of a column from the current row onward
    within each LA_AREA, based on ascending year order.

    Args:
    lf (pl.LazyFrame): Input LazyFrame.
    column_name (str): Name of the column to compute next values for.
    new_column_name (str): Name of the output column to store the result.

    Returns:
    pl.LazyFrame: LazyFrame with an additional column containing the next
        non-null value.
    """
    return lf.sort([DP.LA_AREA, DP.YEAR_AS_INTEGER]).with_columns(
        pl.col(column_name).backward_fill().over(DP.LA_AREA).alias(new_column_name)
    )


def calculated_interpolated_values_in_new_column(
    lf: pl.LazyFrame, new_column_name: str
) -> pl.LazyFrame:
    """
    Calculates interpolated values using linear interpolation between
    the nearest previous and next known observations.

    The interpolation formula used is:

        y = y_prev + (y_next - y_prev) * (x - x_prev) / (x_next - x_prev)

    where:
    - x = current year
    - x_prev = previous year with known value
    - x_next = next year with known value
    - y_prev = previous known value
    - y_next = next known value

    If x_prev == x_next, the function returns the current observed value.

    Args:
    lf (pl.LazyFrame): Input LazyFrame with columns YEAR_AS_INTEGER, PREVIOUS_* columns,
        NEXT_* columns and ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF

    new_column_name (str): Name of the column to store interpolated values.

    Returns:
    pl.LazyFrame: LazyFrame with columns LA_AREA, YEAR_AS_INTEGER and
        new_column_name (interpolated values)

    """
    current_year = pl.col(DP.YEAR_AS_INTEGER)

    previous_year_with_value = pl.col(
        DP.PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
    )
    next_year_with_value = pl.col(
        DP.NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
    )

    current_value = pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    previous_value = pl.col(DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF)
    next_value = pl.col(DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF)

    interpolated_expr = (
        pl.when(previous_year_with_value == next_year_with_value)
        .then(current_value)
        .otherwise(
            previous_value
            + (next_value - previous_value)
            * (current_year - previous_year_with_value)
            / (next_year_with_value - previous_year_with_value)
        )
    )

    return lf.with_columns(interpolated_expr.alias(new_column_name)).select(
        DP.LA_AREA, DP.YEAR_AS_INTEGER, new_column_name
    )
