import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def determine_areas_including_carers_on_adass(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = add_column_with_year_as_integer(direct_payments_df)
    direct_payments_df = calculate_proportion_of_dprs_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_dprs_at_year_end(direct_payments_df)
    direct_payments_df = determine_if_adass_base_is_closer_to_total_dpr_or_su_only(direct_payments_df)
    direct_payments_df = calculate_value_if_adass_base_is_closer_to_total_dpr(direct_payments_df)
    direct_payments_df = calculate_value_if_adass_base_is_closer_to_su_only(direct_payments_df)
    direct_payments_df = allocate_proportions(direct_payments_df)
    direct_payments_df = remove_outliers(direct_payments_df)

    return direct_payments_df


def add_column_with_year_as_integer(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.YEAR_AS_INTEGER,
        F.col(DP.YEAR_AS_INTEGER).cast("int"),
    )
    return direct_payments_df


def calculate_proportion_of_dprs_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_OF_DPR_EMPLOYING_STAFF,
        F.col(DP.DPRS_EMPLOYING_STAFF_ADASS) / F.col(DP.DPRS_ADASS),
    )
    return df


def calculate_total_dprs_at_year_end(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.TOTAL_DPRS_AT_YEAR_END,
        F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END) + F.col(DP.CARER_DPRS_AT_YEAR_END),
    )
    return df


def determine_if_adass_base_is_closer_to_total_dpr_or_su_only(
    df: DataFrame,
) -> DataFrame:
    df = calculate_difference_between_bases(df, DP.DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF, DP.TOTAL_DPRS_AT_YEAR_END)
    df = calculate_difference_between_bases(
        df,
        DP.DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF,
        DP.SERVICE_USER_DPRS_AT_YEAR_END,
    )
    df = allocate_which_base_is_closer(df)
    return df


def calculate_difference_between_bases(
    df: DataFrame,
    new_column: str,
    ascof_column: str,
) -> DataFrame:
    df = df.withColumn(
        new_column,
        F.abs(F.col(DP.DPRS_ADASS) - F.col(ascof_column)),
    )
    return df


def allocate_which_base_is_closer(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.CLOSER_BASE,
        F.when(
            F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF) < F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF),
            F.lit(Values.TOTAL_DPRS),
        )
        .when(
            F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF) > F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF),
            F.lit(Values.SU_ONLY_DPRS),
        )
        .otherwise(F.lit(Values.TOTAL_DPRS)),
    )
    return df


def calculate_value_if_adass_base_is_closer_to_total_dpr(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_IF_TOTAL_DPR_CLOSER,
        F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF)
        * F.col(DP.TOTAL_DPRS_AT_YEAR_END)
        / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
    )
    return df


def calculate_value_if_adass_base_is_closer_to_su_only(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER,
        (
            (F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF) * F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END))
            + (F.col(DP.CARER_DPRS_AT_YEAR_END) * Config.CARERS_EMPLOYING_PERCENTAGE)
        )
        / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
    )
    return df


def allocate_proportions(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.PROPORTION_ALLOCATED,
        F.when(F.col(DP.CLOSER_BASE) == Values.TOTAL_DPRS, F.col(DP.PROPORTION_IF_TOTAL_DPR_CLOSER),).when(
            F.col(DP.CLOSER_BASE) == Values.SU_ONLY_DPRS,
            F.col(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER),
        ),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.PROPORTION_ALLOCATED,
        F.when(
            F.col(DP.PROPORTION_ALLOCATED) < Config.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD,
            F.col(DP.PROPORTION_ALLOCATED),
        ).otherwise(F.col(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER)),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.col(DP.PROPORTION_ALLOCATED)),
    )
    return direct_payments_df


def remove_outliers(df: DataFrame) -> DataFrame:
    df = create_column_to_mark_outliers_for_removal(df)
    df = identify_values_below_zero_or_above_one(df)
    df = identify_extreme_values_when_only_value_in_la_area(df)
    df = calculate_mean_proportion_of_service_users_employing_staff(df)
    df = identify_outliers_using_threshold_value(df)
    df = identify_extreme_values_not_following_a_trend_in_most_recent_year(df)
    df = retain_cases_where_latest_number_we_know_is_not_outlier(df)
    df = remove_identified_outliers(df)
    return df


def create_column_to_mark_outliers_for_removal(df: DataFrame) -> DataFrame:
    df = df.withColumn(DP.OUTLIERS_FOR_REMOVAL, F.lit(Values.RETAIN))
    return df


def identify_values_below_zero_or_above_one(df: DataFrame) -> DataFrame:
    # TODO:
    # if proportion is over 1 or below 0, mark for removal
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 1.0)
            | (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.0),
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def identify_extreme_values_when_only_value_in_la_area(df: DataFrame) -> DataFrame:
    # TODO:
    # group by la and count number of years with proportion
    count_df = df.groupBy(DP.LA_AREA).agg(
        F.count(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .cast("integer")
        .alias(DP.COUNT_OF_YEARS_WITH_PROPORTION),
    )
    count_df = count_df.select(DP.LA_AREA, DP.COUNT_OF_YEARS_WITH_PROPORTION)
    # join count of years with proportion to df
    df = df.join(count_df, on=DP.LA_AREA, how="left")
    # if count of years with proportion is 1 and proportion is not null and value is above 0.85 or below 0.15, mark for removal
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.COUNT_OF_YEARS_WITH_PROPORTION) == 1)
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())
            & (
                (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 0.85)
                | (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.15)
            ),
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def calculate_mean_proportion_of_service_users_employing_staff(
    df: DataFrame,
) -> DataFrame:
    means_df = df.groupBy(DP.LA_AREA).mean(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    df = df.join(means_df, on=DP.LA_AREA, how="left")
    df = df.withColumnRenamed(
        DP.GROUPED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    return df


def identify_outliers_using_threshold_value(
    df: DataFrame,
) -> DataFrame:
    # TODO
    # If distance from mean is over threshold, mark for removal
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            F.abs(
                F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
                - F.col(DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
            )
            >= Config.ADASS_PROPORTION_OUTLIER_THRESHOLD,
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def identify_extreme_values_not_following_a_trend_in_most_recent_year(df: DataFrame) -> DataFrame:
    # TODO
    # group by la
    filtered_df = df.where(F.col(DP.YEAR_AS_INTEGER) == 2021)
    # duplicate 2021 column
    filtered_df = filtered_df.withColumn("2021_data", F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF))
    filtered_df = filtered_df.select(DP.LA_AREA, "2021_data")
    # join la and 2021 data back into df
    df = df.join(filtered_df, on=DP.LA_AREA, how="left")
    # if year is 2022 and 2021 data column is not null and proportion column is not null, and proportion is >0.9 or <0.1, and abs diff between 2022 and 2021 >0.3, mark for removal
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.YEAR_AS_INTEGER) == 2022)
            & (F.col("2021_data").isNotNull())
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())
            & (
                (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 0.9)
                | (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.1)
            )
            & (F.abs(F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) - F.col("2021_data")) > 0.3),
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def retain_cases_where_latest_number_we_know_is_not_outlier(df: DataFrame) -> DataFrame:
    # TODO
    # add column with year of latest data (see extrapolation)
    df = add_column_with_last_year_of_data(df, "last_year_containing_raw_data")
    # add column with proportion of latest data (see extrapolation)
    df = add_data_point_from_given_year_of_data(
        df, "last_year_containing_raw_data", DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, "last_raw_data_point"
    )
    # if year of latest data = year and proportion is >0.25 or <0.75, mark to retain
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.YEAR_AS_INTEGER) == F.col("last_year_containing_raw_data"))
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 0.25)
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.75),
            F.lit(Values.RETAIN),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    df.select(
        DP.LA_AREA,
        DP.YEAR_AS_INTEGER,
        DP.OUTLIERS_FOR_REMOVAL,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        "last_year_containing_raw_data",
        "last_raw_data_point",
    ).sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).show()
    return df


def add_column_with_last_year_of_data(
    direct_payments_df: DataFrame,
    column_name: str,
) -> DataFrame:
    populated_df = filter_to_locations_with_known_service_users_employing_staff(direct_payments_df)
    last_submission_date_df = determine_last_year_with_data(populated_df, column_name)

    direct_payments_df = direct_payments_df.join(last_submission_date_df, DP.LA_AREA, "left")

    return direct_payments_df


def filter_to_locations_with_known_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = direct_payments_df.where(F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())
    return populated_df


def determine_last_year_with_data(
    populated_df: DataFrame,
    column_name: str,
) -> DataFrame:
    last_submission_date_df = populated_df.groupBy(DP.LA_AREA).agg(
        F.max(DP.YEAR_AS_INTEGER).cast("integer").alias(column_name),
    )
    return last_submission_date_df


def add_data_point_from_given_year_of_data(
    direct_payments_df: DataFrame,
    year_of_data_to_add: str,
    original_column: str,
    new_column: str,
) -> DataFrame:
    df = direct_payments_df.where(F.col(year_of_data_to_add) == F.col(DP.YEAR_AS_INTEGER))
    df = df.withColumnRenamed(original_column, new_column)
    df = df.select(DP.LA_AREA, new_column)

    direct_payments_df = direct_payments_df.join(df, [DP.LA_AREA], "left")

    return direct_payments_df


def remove_identified_outliers(df: DataFrame) -> DataFrame:
    # TODO
    # if not marked for removal, retain proportion, otherwise remove
    df = df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.OUTLIERS_FOR_REMOVAL) == Values.RETAIN, F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        ).otherwise(F.lit(None)),
    )
    return df
