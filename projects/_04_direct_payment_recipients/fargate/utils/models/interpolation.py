import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_interpolation(
    direct_payments_lf: pl.LazyFrame,
) -> pl.LazyFrame:

    # filter to locations with known service users employing staff
    known_service_users_employing_staff_lf = direct_payments_lf.select(
        [
            DP.LA_AREA,
            DP.YEAR_AS_INTEGER,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        ]
    ).filter(
        pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).is_not_null()
    )

    # calculate first and last submission year per la area
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

    # convert first and last known years into exploded lf

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

    # merge_known_values_with_exploded_dates
    all_dates_lf = all_dates_lf.rename({DP.INTERPOLATION_YEAR: DP.YEAR_AS_INTEGER})

    all_dates_lf = all_dates_lf.join(
        known_service_users_employing_staff_lf, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )

    # add_year_with_data_for_known_service_users_employing_staff

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

    # join_interpolation_into_direct_payments_lf

    direct_payments_lf = direct_payments_lf.join(
        all_dates_lf, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )

    return direct_payments_lf


def interpolate_values_for_all_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
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
    return lf.sort([DP.LA_AREA, DP.YEAR_AS_INTEGER]).with_columns(
        pl.col(column_name).forward_fill().over(DP.LA_AREA).alias(new_column_name)
    )


def get_next_value_in_new_column(
    lf: pl.LazyFrame, column_name: str, new_column_name: str
) -> pl.LazyFrame:
    return lf.sort([DP.LA_AREA, DP.YEAR_AS_INTEGER]).with_columns(
        pl.col(column_name).backward_fill().over(DP.LA_AREA).alias(new_column_name)
    )


def calculated_interpolated_values_in_new_column(
    lf: pl.LazyFrame, new_column_name: str
) -> pl.LazyFrame:
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
