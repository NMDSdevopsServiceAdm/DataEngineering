from typing import List, Optional, Union

from pyspark.ml.feature import Bucketizer
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

key: str = "key"
value: str = "value"
import_date_s3_uri_format = "yyyyMMdd"
pir_submission_date_uri_format = "dd-MMM-yy"


def apply_categorical_labels(
    df: DataFrame, labels: dict, column_names: list, add_as_new_column: bool = True
) -> DataFrame:
    """
    Apply categorical label mappings to one or more columns using a join-based lookup.

    For each column in `column_names`, the corresponding dictionary in `labels`
    is converted to a Spark DataFrame and joined to `df` to map codes to labels.
    Partial mappings are supported: unmapped values will be preserved.

    Labels can either be added as new columns or replace the original columns.

    Args:
        df (DataFrame): Input Spark DataFrame.
        labels (dict): Dictionary of column-to-mapping dictionaries.
        column_names (list): List of column names to apply label mappings to.
        add_as_new_column (bool, optional): If True, adds a new column with
            "_labels" suffix. If False, replaces the original column.
            Defaults to True.

    Returns:
        DataFrame: DataFrame with categorical labels applied. Unmapped values
        are preserved.
    """
    spark = utils.get_spark()

    for column_name in column_names:
        mapping_schema = StructType(
            [
                StructField(column_name, StringType(), True),
                StructField(f"{column_name}_labels", StringType(), True),
            ]
        )
        mapping_df = spark.createDataFrame(
            labels[column_name].items(), schema=mapping_schema
        )

        df = df.join(mapping_df, on=column_name, how="left")

        merged_col = F.coalesce(F.col(f"{column_name}_labels"), F.col(column_name))

        if add_as_new_column:
            df = df.withColumn(f"{column_name}_labels", merged_col)
        else:
            df = df.withColumn(column_name, merged_col).drop(f"{column_name}_labels")

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


# converted to polars -> polars_utils\cleaning_utils.column_to_date
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


# converted to polars -> polars_utils.cleaning_utils.add_aligned_date_column
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


# converted to polars -> polars_utils.cleaning_utils.add_aligned_date_column
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


# converted to polars -> polars_utils.cleaning_utils.add_aligned_date_column
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


# converted to polars -> polars_utils.cleaning_utils.add_aligned_date_column
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


# converted to polars -> polars_utils.cleaning_utils.reduce_dataset_to_earliest_file_per_month
def reduce_dataset_to_earliest_file_per_month(df: DataFrame) -> DataFrame:
    """
    Reduce the dataset to the first file of every month.

    This function identifies the date of the first import date in each month and then filters the dataset to those import dates only.

    Args:
        df (DataFrame): A dataframe containing the partition keys year, month and day.

    Returns:
        DataFrame: A dataframe with only the first import date of each month.
    """
    earliest_date_in_month = "earliest_date_in_month"

    w = Window.partitionBy(
        F.year(IndCQC.cqc_location_import_date),
        F.month(IndCQC.cqc_location_import_date),
    )

    df = (
        df.withColumn(
            earliest_date_in_month,
            F.min(F.col(IndCQC.cqc_location_import_date)).over(w),
        )
        .where(F.col(IndCQC.cqc_location_import_date) == F.col(earliest_date_in_month))
        .drop(earliest_date_in_month)
    )

    return df


def cast_to_int(df: DataFrame, column_names: list) -> DataFrame:
    for column in column_names:
        df = df.withColumn(column, df[column].cast(IntegerType()))
    return df


# converted to polars -> polars_utils.cleaning_utils.calculate_filled_posts_per_bed_ratio
def calculate_filled_posts_per_bed_ratio(
    input_df: DataFrame, filled_posts_column: str, new_column_name: str
) -> DataFrame:
    """
    Add a column with the filled post per bed ratio for care homes.

    Args:
        input_df (DataFrame): A dataframe containing the given column, care_home and numberofbeds.
        filled_posts_column (str): The name of the column to use for calculating the ratio.
        new_column_name (str): The name to give the new column.

    Returns:
        DataFrame: The same dataframe with an additional column contianing the filled posts per bed ratio for care homes.
    """
    input_df = input_df.withColumn(
        new_column_name,
        F.when(
            F.col(IndCQC.care_home) == CareHome.care_home,
            F.col(filled_posts_column) / F.col(IndCQC.number_of_beds),
        ),
    )

    return input_df


# not converting this function to polars, use '.mul()' instead
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
    order_by: List[Column],
) -> DataFrame:
    """
    Remove duplicate rows using custom window ordering.

    Keeps the first row within each duplicate group according to the
    provided ordering expressions.

    Args:
        df (DataFrame): The DataFrame to remove duplicates from.
        columns_to_identify_duplicates (List[str]): Columns defining duplicate groups.
        order_by (List[Column]): List of PySpark column expressions defining sort priority.

    Returns:
        DataFrame: A DataFrame with duplicates removed.
    """
    temp_col = "row_number"

    return (
        df.withColumn(
            temp_col,
            F.row_number().over(
                Window.partitionBy(*columns_to_identify_duplicates).orderBy(*order_by)
            ),
        )
        .where(F.col(temp_col) == 1)
        .drop(temp_col)
    )


# converted to polars -> polars_utils.cleaning_utils.create_banded_bed_count_column
def create_banded_bed_count_column(
    input_df: DataFrame, new_col: str, splits: List[float]
) -> DataFrame:
    """
    Creates a new column in the input DataFrame that categorises the number of beds into defined bands.

    This function uses a Bucketizer to categorise the number of beds into specified bands.
    The banded bed counts are joined into the original DataFrame.

    If the location is non-res then zero is returned.

    Args:
        input_df (DataFrame): The DataFrame containing the column 'number_of_beds' to be banded.
        new_col (str): The name of the output column with the banded values.
        splits (List[float]): The list of split points for bucketing (must be strictly increasing).

    Returns:
        DataFrame: A new DataFrame that includes the original data along with a new column 'number_of_beds_banded'.
    """
    zero: float = 0.0

    number_of_beds_df = (
        input_df.select(IndCQC.number_of_beds)
        .where(F.col(IndCQC.number_of_beds).isNotNull())
        .dropDuplicates()
    )

    set_banded_boundaries = Bucketizer(
        splits=splits,
        inputCol=IndCQC.number_of_beds,
        outputCol=new_col,
    )

    number_of_beds_with_bands_df = set_banded_boundaries.setHandleInvalid(
        "keep"
    ).transform(number_of_beds_df)

    output_df = input_df.join(
        number_of_beds_with_bands_df, IndCQC.number_of_beds, "left"
    )

    return output_df.withColumn(
        new_col,
        F.when(
            F.col(IndCQC.care_home) == CareHome.not_care_home, F.lit(zero)
        ).otherwise(F.col(new_col)),
    )
