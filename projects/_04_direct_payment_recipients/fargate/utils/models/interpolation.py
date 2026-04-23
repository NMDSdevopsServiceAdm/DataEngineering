import polars as pl
import sys

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
    first_and_last_submission_year_lf = known_service_users_employing_staff_lf.group_by(
        DP.LA_AREA
    ).agg(
        [
            pl.col(DP.YEAR_AS_INTEGER)
            .min()
            .cast(pl.Int32)
            .alias(DP.FIRST_SUBMISSION_YEAR),
            pl.col(DP.YEAR_AS_INTEGER)
            .max()
            .cast(pl.Int32)
            .alias(DP.LAST_SUBMISSION_YEAR),
        ]
    )

    # convert first and last known years into exploded lf

    all_dates_lf = (
        first_and_last_submission_year_lf.with_columns(
            pl.int_ranges(
                pl.col(DP.FIRST_SUBMISSION_YEAR),
                pl.col(DP.LAST_SUBMISSION_YEAR) + 1,  # inclusive
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


# def filter_to_locations_with_known_service_users_employing_staff(
#     df: DataFrame,
# ) -> DataFrame:
#     df = df.select(
#         DP.LA_AREA,
#         DP.YEAR_AS_INTEGER,
#         DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
#     )
#     df = df.where(
#         F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull()
#     )
#     return df


# def calculate_first_and_last_submission_year_per_la_area(
#     df: DataFrame,
# ) -> DataFrame:
#     df = df.groupBy(DP.LA_AREA).agg(
#         F.min(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.FIRST_SUBMISSION_YEAR),
#         F.max(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.LAST_SUBMISSION_YEAR),
#     )
#     return df


# def convert_first_and_last_known_years_into_exploded_df(
#     df: DataFrame,
# ) -> DataFrame:
#     create_list_of_equally_spaced_points_between_start_and_finish_years_udf = F.udf(
#         create_list_of_equally_spaced_points_between_start_and_finish_years,
#         ArrayType(LongType()),
#     )
#     df = df.withColumn(
#         DP.INTERPOLATION_YEAR,
#         F.explode(
#             create_list_of_equally_spaced_points_between_start_and_finish_years_udf(
#                 DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR
#             )
#         ),
#     )

#     df = df.drop(DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR)
#     return df


# def create_list_of_equally_spaced_points_between_start_and_finish_years(
#     start_year: int, finish_year: int
# ) -> int:
#     year_step = 1
#     years = range(int((finish_year - start_year) / year_step) + 1)
#     array_of_years = [start_year + year_step * year for year in years]
#     return array_of_years


# def merge_known_values_with_exploded_dates(
#     all_dates_df: DataFrame, known_service_users_employing_staff_df: DataFrame
# ) -> DataFrame:
#     all_dates_df = all_dates_df.withColumnRenamed(
#         DP.INTERPOLATION_YEAR, DP.YEAR_AS_INTEGER
#     )
#     merged_df = all_dates_df.join(
#         known_service_users_employing_staff_df, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
#     )

#     merged_df = add_year_with_data_for_known_service_users_employing_staff(merged_df)
#     return merged_df


# def add_year_with_data_for_known_service_users_employing_staff(
#     df: DataFrame,
# ) -> DataFrame:
#     df = df.withColumn(
#         DP.estimated_proportion_of_service_users_employing_staff_year_provided,
#         F.when(
#             (
#                 F.col(
#                     DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
#                 ).isNotNull()
#             ),
#             F.col(DP.YEAR_AS_INTEGER),
#         ).otherwise(F.lit(None)),
#     )
#     return df


def interpolate_values_for_all_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    lf = input_previous_and_next_values_into_df(lf)
    lf = calculated_interpolated_values_in_new_column(
        lf, DP.ESTIMATE_USING_INTERPOLATION
    )
    return lf


def input_previous_and_next_values_into_df(lf: pl.LazyFrame) -> pl.LazyFrame:
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
    return lf


def get_previous_value_in_column(
    lf: pl.LazyFrame, column_name: str, new_column_name: str
) -> pl.LazyFrame:
    return lf.sort([DP.LA_AREA, DP.YEAR_AS_INTEGER]).with_columns(
        pl.col(column_name)
        .forward_fill()  # last non-null up to current row
        .over(DP.LA_AREA)
        .alias(new_column_name)
    )


def get_next_value_in_new_column(
    lf: pl.LazyFrame, column_name: str, new_column_name: str
) -> pl.LazyFrame:
    return lf.sort([DP.LA_AREA, DP.YEAR_AS_INTEGER]).with_columns(
        pl.col(column_name)
        .backward_fill()  # first non-null from current row onward
        .over(DP.LA_AREA)
        .alias(new_column_name)
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
