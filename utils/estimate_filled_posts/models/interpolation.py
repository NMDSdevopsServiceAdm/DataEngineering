import sys
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import ArrayType, LongType, FloatType

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_interpolation(
    df: DataFrame, column_to_interpolate: str, new_column_name: str
) -> DataFrame:
    """
    Adds a new column with interpolated values.

    The function controls the stages within the interpolation process.

    Args:
        df (DataFrame): A dataframe with a column to interpolate.
        column_to_interpolate (str): The name of the column that needs interpolating.
        new_column_name (str): The name of the new column with interpolated values.

    Returns:
        DataFrame: A dataframe with a new column containing interpolated values.
    """
    known_value_df = filter_to_locations_with_a_known_value(df, column_to_interpolate)

    first_and_last_submission_date_df = (
        calculate_first_and_last_submission_date_per_location(known_value_df)
    )

    all_dates_df = convert_first_and_last_known_years_into_exploded_df(
        first_and_last_submission_date_df
    )

    all_dates_df = merge_known_values_with_exploded_dates(
        all_dates_df, known_value_df, column_to_interpolate
    )

    all_dates_df = interpolate_values_for_all_dates(
        all_dates_df, column_to_interpolate, new_column_name
    )

    df = leftouter_join_on_locationid_and_unix_time(df, all_dates_df)

    return df


def filter_to_locations_with_a_known_value(
    df: DataFrame, column_to_interpolate: str
) -> DataFrame:
    """
    Filter the given column to non-null data.

    This function reduces the dataset to 3 columns and then filters the rows to non-null values.

    Args:
        df (DataFrame): A dataframe with the columns location_id, unix_time and the column that needs interpolating.
        column_to_interpolate (str): The name of the column that needs interpolating.

    Retuns:
        DataFrame: A dataframe with 3 columns and only rows with non-null values.
    """
    df = df.select(IndCqc.location_id, IndCqc.unix_time, column_to_interpolate)

    df = df.where(F.col(column_to_interpolate).isNotNull())
    return df


def calculate_first_and_last_submission_date_per_location(df: DataFrame) -> DataFrame:
    """
    Calculate the fist and last submission data for each location.

    This function creates a dataframe with the first and last submission date for each location id.

    Args:
        df (DataFrame): A dataframe with the columns location_id and unix_time.

    Retuns:
        DataFrame: A dataframe with the first and last submission date for each location id.
    """
    df = df.groupBy(IndCqc.location_id).agg(
        F.min(IndCqc.unix_time).cast("integer").alias(IndCqc.first_submission_time),
        F.max(IndCqc.unix_time).cast("integer").alias(IndCqc.last_submission_time),
    )
    return df


def convert_first_and_last_known_years_into_exploded_df(df: DataFrame) -> DataFrame:
    """
    Creates an exploded dataframe based on the first and last known years.

    This function creates a dataframe with rows in between the first and last submission date for each location id.

    Args:
        df (DataFrame): A dataframe with the columns location_id and unix_time, first_submission_time, and last_submission_time.

    Retuns:
        DataFrame: A dataframe with rows in between the first and last submission date for each location id.
    """
    date_range_udf = F.udf(create_date_range, ArrayType(LongType()))

    df = df.withColumn(
        IndCqc.unix_time,
        F.explode(
            date_range_udf(IndCqc.first_submission_time, IndCqc.last_submission_time)
        ),
    ).drop(IndCqc.first_submission_time, IndCqc.last_submission_time)

    return df


def create_date_range(
    unix_start_time: int, unix_finish_time: int, step_size_in_days: int = 1
) -> list[int]:
    """
    Return a list of equally spaced points between unix_start_time and unix_finish_time with set stepsizes.

    Args:
        unix_start_time (int): A unix time representing the start of a date range.
        unix_finish_time (int): A unix time representing the end of a date range.
        step_size_in_days (int): A number of days to use as an interval.

    Retuns:
        list[int]: A list of unix times between the start and end dates.
    """
    unix_time_step = convert_days_to_unix_time(step_size_in_days)

    return [
        unix_start_time + unix_time_step * x
        for x in range(int((unix_finish_time - unix_start_time) / unix_time_step) + 1)
    ]


def merge_known_values_with_exploded_dates(
    all_dates_df: DataFrame, known_value_df: DataFrame, column_to_interpolate: str
) -> DataFrame:
    """
    Merges known values into the dataframes of exploded dates.

    Args:
        all_dates_df (DataFrame): A dataframe with the columns location_id, unix_time and the column that needs interpolating.
        known_value_df (DataFrame): A dataframe with the columns location_id, unix_time and the column that needs interpolating.
        column_to_interpolate (str): The name of the column that needs interpolating.

    Retuns:
        DataFrame: A dataframe with exploded dates and known values where applicable.
    """
    all_dates_df = leftouter_join_on_locationid_and_unix_time(
        all_dates_df, known_value_df
    )
    all_dates_df = add_unix_time_for_known_value(all_dates_df, column_to_interpolate)
    return all_dates_df


def leftouter_join_on_locationid_and_unix_time(
    df: DataFrame, other_df: DataFrame
) -> DataFrame:
    """
    Joins dataframes on location_id and unix_time.

    Args:
        df (DataFrame): A dataframe with the columns location_id and unix_time.
        other_df (DataFrame): A dataframe with the columns location_id and unix_time to join into the first dataframe.

    Retuns:
        DataFrame: A dataframe with the whole of df and the relevent rows of other_df.
    """
    return df.join(other_df, [IndCqc.location_id, IndCqc.unix_time], "leftouter")


def add_unix_time_for_known_value(
    df: DataFrame, column_to_interpolate: str
) -> DataFrame:
    """
    Add a column with unix time when the interpolation value is known.

    Args:
        df (DataFrame): A dataframe with the column unix_time and the column that needs interpolating.
        column_to_interpolate (str): The name of the column that needs interpolating.

    Retuns:
        DataFrame: A dataframe with an additional column with unix time when the interpolation value is known.
    """
    df = df.withColumn(
        IndCqc.value_unix_time,
        F.when(
            (F.col(column_to_interpolate).isNotNull()),
            F.col(IndCqc.unix_time),
        ).otherwise(F.lit(None)),
    )
    return df


def interpolate_values_for_all_dates(
    df: DataFrame, column_to_interpolate: str, new_column_name: str
) -> DataFrame:
    """
    Interpolates all values for a prepared dataframe.

    Args:
        df (DataFrame): A prepared dataframe with data to interpolate.
        column_to_interpolate (str): The name of the column that needs interpolating.
        new_column_name (str): The name of the new column with interpolated values.

    Retuns:
        DataFrame: A dataframe with an additional column with interpolated values.
    """
    df = input_previous_and_next_values_into_df(df, column_to_interpolate)
    df = calculate_interpolated_values_in_new_column(
        df, new_column_name, column_to_interpolate
    )
    return df


def input_previous_and_next_values_into_df(
    df: DataFrame, column_to_interpolate: str
) -> DataFrame:
    """
    Add previous and next values into dataframe.

    This function controls the flow of adding previous and next values into the dataframe which will be used for calculating the interpolated values.

    Args:
        df (DataFrame): A prepared dataframe with data to interpolate.
        column_to_interpolate (str): The name of the column that needs interpolating.

    Retuns:
        DataFrame: A dataframe with additional columns with previous and next dates and values to interpolate.
    """
    df = get_previous_value_in_column(df, column_to_interpolate, IndCqc.previous_value)
    df = get_previous_value_in_column(
        df, IndCqc.value_unix_time, IndCqc.previous_value_unix_time
    )
    df = get_next_value_in_new_column(df, column_to_interpolate, IndCqc.next_value)
    df = get_next_value_in_new_column(
        df, IndCqc.value_unix_time, IndCqc.next_value_unix_time
    )
    return df


def get_previous_value_in_column(
    df: DataFrame, column_name: str, new_column_name: str
) -> DataFrame:
    """
    Adds a new column with the previous value of the given column into the dataframe.

    Args:
        df (DataFrame): A prepared dataframe with data to interpolate.
        column_name (str): The name of the column that needs a previous value retreiving.
        new_column_name (str): The name of the new column to be added.

    Retuns:
        DataFrame: A dataframe with an additional column with the previous value of the given column.
    """
    df = df.withColumn(
        new_column_name,
        F.last(F.col(column_name), ignorenulls=True).over(
            create_window_for_previous_value()
        ),
    )
    return df


def create_window_for_previous_value() -> Window:
    """
    Creates a window for getting the previous value from a column.

    Returns:
        Window: A window for getting the previous value from a column.
    """
    return (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(IndCqc.unix_time)
        .rowsBetween(-sys.maxsize, 0)
    )


def get_next_value_in_new_column(
    df: DataFrame, column_name: str, new_column_name: str
) -> DataFrame:
    """
    Adds a new column with the next value of the given column into the dataframe.

    Args:
        df (DataFrame): A prepared dataframe with data to interpolate.
        column_name (str): The name of the column that needs a next value retreiving.
        new_column_name (str): The name of the new column to be added.

    Retuns:
        DataFrame: A dataframe with an additional column with the next value of the given column.
    """
    df = df.withColumn(
        new_column_name,
        F.first(F.col(column_name), ignorenulls=True).over(
            create_window_for_next_value()
        ),
    )
    return df


def create_window_for_next_value() -> Window:
    """
    Creates a window for getting the next value from a column.

    Returns:
        Window: A window for getting the next value from a column.
    """
    return (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(IndCqc.unix_time)
        .rowsBetween(0, sys.maxsize)
    )


def calculate_interpolated_values_in_new_column(
    df: DataFrame, new_column_name: str, column_to_interpolate: str
) -> DataFrame:
    """
    Calculates interpolated values for a prepared dataframe.

    Args:
        df (DataFrame): A prepared dataframe with data to interpolate.
        new_column_name (str): The name of the new column with interpolated values.
        column_to_interpolate (str): The name of the column that needs interpolating.

    Retuns:
        DataFrame: A dataframe with 3 columns: location_id, unix_time, and the interpolated column.
    """
    interpol_udf = F.udf(interpolate_values, FloatType())

    df = df.withColumn(
        new_column_name,
        interpol_udf(
            IndCqc.unix_time,
            IndCqc.previous_value_unix_time,
            IndCqc.next_value_unix_time,
            column_to_interpolate,
            IndCqc.previous_value,
            IndCqc.next_value,
        ),
    )
    df = df.select(IndCqc.location_id, IndCqc.unix_time, new_column_name)

    return df


def interpolate_values(
    unix_time: str,
    previous_value_unix_time: str,
    next_value_unix_time: str,
    value: str,
    previous_value: str,
    next_value: str,
) -> float:
    """
    Formula for calculating interpolated values.

    Args:
        unix_time (str):  The name of the column containing the current unix time.
        previous_value_unix_time (str): The name of the column containing the previous unix time value.
        next_value_unix_time (str): The name of the column containing the next unix time value.
        value (str): The name of the column containing the current value to interpolate.
        previous_value (str): The name of the column containing the previous value to interpolate
        next_value (str): The name of the column containing the next value to interpolate
    """
    if previous_value_unix_time == next_value_unix_time:
        return value
    else:
        value_per_unix_time_ratio = (next_value - previous_value) / (
            next_value_unix_time - previous_value_unix_time
        )
        return previous_value + value_per_unix_time_ratio * (
            unix_time - previous_value_unix_time
        )
