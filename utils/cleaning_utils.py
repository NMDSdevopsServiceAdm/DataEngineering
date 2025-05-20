from typing import Optional, Union, List

from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import CareHome

key: str = "key"
value: str = "value"
import_date_s3_uri_format = "yyyyMMdd"
pir_submission_date_uri_format = "dd-MMM-yy"


def apply_categorical_labels(
    df: DataFrame,
    labels: dict,
    column_names: list,
    add_as_new_column: bool = True,
) -> DataFrame:
    for column_name in column_names:
        labels_dict = labels[column_name]
        if add_as_new_column is True:
            new_column_name = column_name + "_labels"
            df = df.withColumn(new_column_name, F.col(column_name))
            df = df.replace(labels_dict, subset=new_column_name)
        elif add_as_new_column is False:
            df = df.replace(labels_dict, subset=column_name)
    return df


def set_column_bounds(
    df: DataFrame,
    col_name: str,
    new_col_name: str,
    lower_limit: Optional[Union[int, float]] = None,
    upper_limit: Optional[Union[int, float]] = None,
) -> DataFrame:
    """
    Creates a new column based on a previous column, dropping values that exist outside of the lower and upper limits, where provided.

    Args:
        df (DataFrame): The DataFrame containing the column to bound
        col_name (str): The name of the column to be bounded, as a string
        new_col_name (str): What the newly created column will be called, as a string
        lower_limit (Optional[Union[int,float]]): The value of the lower limit.
        upper_limit (Optional[Union[int,float]]): The value of the upper limit.

    Returns:
        DataFrame: The DataFrame with the new column only containing numerical values that fell within the bounds.

    Raises:
        ValueError: If both limits are comparable numerical types, and the upper limit is lower than the lower limit.
    """
    if lower_limit is None and upper_limit is None:
        return df

    elif lower_limit is not None and upper_limit is not None:
        if lower_limit > upper_limit:
            raise ValueError(
                f"Lower limit ({lower_limit}) must be lower than upper limit ({upper_limit})"
            )

    if lower_limit is not None:
        df = df.withColumn(
            new_col_name,
            F.when(F.col(col_name) >= lower_limit, F.col(col_name)).otherwise(None),
        )
        col_name = new_col_name

    if upper_limit is not None:
        df = df.withColumn(
            new_col_name,
            F.when(F.col(col_name) <= upper_limit, F.col(col_name)).otherwise(None),
        )

    return df


def set_bounds_for_columns(
    df: DataFrame,
    col_names: list,
    new_col_names: list,
    lower_limit=None,
    upper_limit=None,
):
    if len(col_names) != len(new_col_names):
        raise Exception(
            f"Column list size ({len(col_names)}) must match new column list size ({len(new_col_names)})"
        )

    for col, new_col in zip(col_names, new_col_names):
        df = set_column_bounds(df, col, new_col, lower_limit, upper_limit)

    return df


def column_to_date(
    df: DataFrame,
    column_to_format: str,
    new_column: str = None,
    string_format: str = import_date_s3_uri_format,
) -> DataFrame:
    if new_column is None:
        new_column = column_to_format

    new_df = df.withColumn(new_column, F.to_date(column_to_format, string_format))

    return new_df


def align_import_dates(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_column: str,
    secondary_column: str,
) -> DataFrame:
    possible_matches = cross_join_unique_dates(
        primary_df, secondary_df, primary_column, secondary_column
    )

    aligned_dates = determine_best_date_matches(
        possible_matches, primary_column, secondary_column
    )

    return aligned_dates


def cross_join_unique_dates(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_column: str,
    secondary_column: str,
) -> DataFrame:
    primary_dates = primary_df.select(primary_column).dropDuplicates()
    secondary_dates = secondary_df.select(secondary_column).dropDuplicates()

    possible_matches = primary_dates.crossJoin(secondary_dates).repartition(
        primary_column
    )
    return possible_matches


def determine_best_date_matches(
    possible_matches: DataFrame, primary_column: str, secondary_column: str
) -> DataFrame:
    date_diff: str = "date_diff"
    min_date_diff: str = "min_date_diff"
    possible_matches = possible_matches.withColumn(
        date_diff, F.datediff(primary_column, secondary_column)
    )

    possible_matches = possible_matches.where(possible_matches[date_diff] >= 0)

    w = Window.partitionBy(primary_column).orderBy(date_diff)
    possible_matches = possible_matches.withColumn(
        min_date_diff, F.min(date_diff).over(w)
    )

    aligned_dates = possible_matches.where(
        possible_matches[min_date_diff] == possible_matches[date_diff]
    )

    return aligned_dates.select(primary_column, secondary_column)


def add_aligned_date_column(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_column: str,
    secondary_column: str,
) -> DataFrame:
    aligned_dates = align_import_dates(
        primary_df, secondary_df, primary_column, secondary_column
    )

    primary_df_with_aligned_dates = primary_df.join(
        aligned_dates, primary_column, "left"
    )

    return primary_df_with_aligned_dates


def reduce_dataset_to_earliest_file_per_month(df: DataFrame) -> DataFrame:
    """
    Reduce the dataset to the first file of every month.

    This function identifies the date of the first import date in each month and then filters the dataset to those import dates only.

    Args:
        df (DataFrame): A dataframe containing the partition keys year, month and day.

    Returns:
        DataFrame: A dataframe with only the first import date of each month.
    """
    earliest_day_in_month = "first_day_in_month"
    w = Window.partitionBy(Keys.year, Keys.month).orderBy(Keys.day)
    df = df.withColumn(earliest_day_in_month, F.first(Keys.day).over(w))
    df = df.where(df[earliest_day_in_month] == df[Keys.day]).drop(earliest_day_in_month)
    return df


def cast_to_int(df: DataFrame, column_names: list) -> DataFrame:
    for column in column_names:
        df = df.withColumn(column, df[column].cast(IntegerType()))
    return df


def calculate_filled_posts_per_bed_ratio(
    input_df: DataFrame, filled_posts_column: str
) -> DataFrame:
    """
    Add a column with the filled post per bed ratio for care homes.

    Args:
        input_df (DataFrame): A dataframe containing the given column, care_home and numberofbeds.
        filled_posts_column (str): The name of the column to use for calculating the ratio.

    Returns:
        DataFrame: The same dataframe with an additional column contianing the filled posts per bed ratio for care homes.
    """
    input_df = input_df.withColumn(
        IndCQC.filled_posts_per_bed_ratio,
        F.when(
            F.col(IndCQC.care_home) == CareHome.care_home,
            F.col(filled_posts_column) / F.col(IndCQC.number_of_beds),
        ),
    )

    return input_df


def calculate_filled_posts_from_beds_and_ratio(
    df: DataFrame, ratio_column: str, new_column_name: str
) -> DataFrame:
    """
    Calculate a column with the number of filled posts, based on the number of beds and the given beds ratio column.

    Args:
        df(DataFrame): A dataframe with number_of_beds and a beds ratio column.
        ratio_column(str): The name of the beds ratio column to use.
        new_column_name(str): The name of the column to fill.

    Returns:
        DataFrame: A dataframe with the new calculated filled posts column.
    """
    df = df.withColumn(
        new_column_name,
        F.col(ratio_column) * F.col(IndCQC.number_of_beds),
    )
    return df


def remove_duplicates_based_on_column_order(
    df: DataFrame,
    columns_to_identify_duplicates: List[str],
    column_to_sort_on: str,
    sort_ascending: bool = True,
) -> DataFrame:
    """
    Remove duplicate rows once columns are sorted.

    Args:
        df (DataFrame): The DataFrame to remove duplicates from.
        columns_to_identify_duplicates (List[str]): List of column names used to highlight duplicates.
        column_to_sort_on (str): The name of the column to sort on (sorted in descending order).
        sort_ascending (bool): If true, the column to sort on is sorted ascending, otherwise descending.

    Returns:
        DataFrame: A DataFrame with duplicate location_ids in the same import date removed.
    """
    temp_col = "row_number"
    if sort_ascending == True:
        df = df.withColumn(
            temp_col,
            F.row_number().over(
                Window.partitionBy(columns_to_identify_duplicates).orderBy(
                    column_to_sort_on
                )
            ),
        )
    else:
        df = df.withColumn(
            temp_col,
            F.row_number().over(
                Window.partitionBy(columns_to_identify_duplicates).orderBy(
                    F.desc(column_to_sort_on)
                )
            ),
        )
    df = df.where(F.col(temp_col) == 1).drop(temp_col)
    return df


def create_banded_bed_count_column(input_df: DataFrame) -> DataFrame:
    """
    Creates a new column in the input DataFrame that categorises the number of beds into defined bands.

    This function uses a Bucketizer to categorise the number of beds into specified bands.
    The banded bed counts are joined into the original DataFrame.

    Args:
        input_df (DataFrame): The DataFrame containing the column 'number_of_beds' to be banded.

    Returns:
        DataFrame: A new DataFrame that includes the original data along with a new column 'number_of_beds_banded'.
    """
    number_of_beds_df = (
        input_df.select(IndCQC.number_of_beds)
        .where(F.col(IndCQC.number_of_beds).isNotNull())
        .dropDuplicates()
    )

    set_banded_boundaries = Bucketizer(
        splits=[0, 1, 3, 5, 10, 15, 20, 25, 50, float("Inf")],
        inputCol=IndCQC.number_of_beds,
        outputCol=IndCQC.number_of_beds_banded,
    )

    number_of_beds_with_bands_df = set_banded_boundaries.setHandleInvalid(
        "keep"
    ).transform(number_of_beds_df)

    return input_df.join(number_of_beds_with_bands_df, IndCQC.number_of_beds, "left")


def add_column_for_earliest_import_date_per_dormancy_value(df: DataFrame) -> DataFrame:
    """
    Adds a column that repeats the earliest cqc_location_import_date across rows where dormancy column is the same per location.

    Args:
        df (DataFrame): A dataframe with cqc_location_import_date and dormancy columns.

    Returns:
        DataFrame: A dataframe with additional earliest_import_date_per_dormancy_value column.
    """

    w = Window.partitionBy(IndCQC.location_id, IndCQC.dormancy)

    df = df.withColumn(
        IndCQC.earliest_import_date_per_dormancy_value,
        F.min(IndCQC.cqc_location_import_date).over(w),
    )

    return df
