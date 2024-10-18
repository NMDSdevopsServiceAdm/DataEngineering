from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.column_values.categorical_column_values import CareHome
from utils.utils import select_rows_with_value


def model_non_res_time_registered_for(
    df: DataFrame, number_of_periods: int
) -> DataFrame:
    window = define_window_specifications(number_of_periods)

    care_home_df = select_rows_with_value(df, IndCqc.care_home, CareHome.care_home)
    non_res_df = select_rows_with_value(df, IndCqc.care_home, CareHome.not_care_home)

    non_res_df = add_banded_time_registered_for_into_df_version(non_res_df)

    non_res_df = calculate_average_posts_by_time_registered_for(non_res_df, window)

    combined_df = non_res_df.unionByName(care_home_df, allowMissingColumns=True)

    return combined_df


def define_window_specifications(number_of_periods: int) -> Window:
    """
    Define the Window specification partitioned by primary service column.

    Args:
        number_of_days (int): The number of days to use for the rolling average calculations.

    Returns:
        Window: The required Window specification partitioned by primary service column.
    """
    return (
        Window.partitionBy(IndCqc.related_location)
        .orderBy(F.col("time_registered_for_6_mnths"))
        .rangeBetween(-number_of_periods, 0)
    )


def add_banded_time_registered_for_into_df_version(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "time_registered_for_6_mnths",
        F.floor(
            F.months_between(
                F.col(IndCqc.cqc_location_import_date),
                F.col(IndCqc.imputed_registration_date),
            )
            / 6
        ),
    )

    # base is low at the max possible so cap it one year (2 periods) from the max
    max_value = df.agg(F.max("time_registered_for_6_mnths")).collect()[0][0]

    df = df.withColumn(
        "time_registered_for_6_mnths_capped",
        F.least(F.col("time_registered_for_6_mnths"), F.lit(max_value - 2)),
    )
    return df


def calculate_average_posts_by_time_registered_for(
    df: DataFrame, window: Window
) -> DataFrame:
    df = df.withColumn(
        "rolling_average_posts_by_time_registered_for_model",
        F.avg(IndCqc.ascwds_filled_posts_dedup_clean).over(window),
    )
    return df
