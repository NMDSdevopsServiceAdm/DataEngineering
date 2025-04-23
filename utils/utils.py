import argparse
from typing import List, Any, Generator, Tuple

from pyspark.sql import DataFrame, Column, Window, SparkSession, functions as F


class SetupSpark(object):
    def __init__(self):
        self.spark = None

    def __call__(self):
        if self.spark:
            return self.spark

        self.spark = self.setupSpark()
        return self.spark

    def setupSpark(self) -> SparkSession:
        spark = (
            SparkSession.builder.appName("sfc_data_engineering")
            .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.3")
            .getOrCreate()
        )
        spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
        spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY")
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        return spark


get_spark = SetupSpark()


def get_model_name(path_to_model):
    _, prefix = split_s3_uri(path_to_model)
    return prefix.split("/")[1]


def read_from_parquet(
    data_source: str, selected_columns: List[str] = None
) -> DataFrame:
    """
    Reads data from a parquet file and returns a DataFrame with all/selected columns.

    Args:
        data_source (str): Path to the Parquet file.
        selected_columns (List[str]): Optional - List of column names to select. Defaults to None (all columns).

    Returns:
        DataFrame: A dataframe of the data in the parquet file, with all or selected columns.
    """
    spark_session = get_spark()
    print(f"Reading data from {data_source}")

    df = spark_session.read.parquet(data_source)

    if selected_columns:
        df = df.select(selected_columns)

    return df


def write_to_parquet(
    df: DataFrame, output_dir: str, mode: str = None, partitionKeys=[]
):
    df.write.mode(mode).partitionBy(*partitionKeys).parquet(output_dir)


def format_date_fields(df, date_column_identifier="date", raw_date_format=None):
    date_columns = [column for column in df.columns if date_column_identifier in column]

    for date_column in date_columns:
        if "import_date" in date_column:
            continue
        else:
            df = df.withColumn(date_column, F.to_date(date_column, raw_date_format))

    return df


def split_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Splits an S3 URI into bucket name and prefix.

    Args:
        uri (str): The S3 URI to split.

    Returns:
        Tuple[str, str]: A tuple containing the bucket name and prefix."""
    stripped_uri = uri.replace("s3://", "", 1)
    bucket, key = stripped_uri.split("/", 1)
    return bucket, key


def create_unix_timestamp_variable_from_date_column(
    df: DataFrame, date_col: str, date_format: str, new_col_name: str
) -> DataFrame:
    return df.withColumn(
        new_col_name, F.unix_timestamp(F.col(date_col), format=date_format)
    )


def convert_days_to_unix_time(days: int):
    NUMBER_OF_SECONDS_IN_ONE_DAY = 86400
    return days * NUMBER_OF_SECONDS_IN_ONE_DAY


def collect_arguments(*args: Any) -> Generator[Any, None, None]:
    """
    Creates a new parser, and for each arg in the provided args parameter returns a Namespace object, and uses vars() function to convert the namespace to a dictionary,
    where the keys are constructed from the symbolic names, and the values from the information about the object that each name references.

    Args:
        *args (Any): This is intended to be used to contain parsed arguments when run at command line, and is generally to contain keys and values as a tuple.

    Returns:
        Generator[Any, None, None]: A generator used for parsing parsed parameters.

    Examples:
    >>> single_parameter, *_ = collect_arguments(("--single_parameter","This is how you read a single parameter"))
    >>> (parameter_1, parameter_2) = collect_arguments(("--parameter_1","parameter_1 help text"),("--parameter_2","parameter_2 help text for non-required parameter", False))
    """
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(
            arg[0],
            help=arg[1],
            required=True,
        )

    parsed_args, _ = parser.parse_known_args()

    return (vars(parsed_args)[arg[0][2:]] for arg in args)


def latest_datefield_for_grouping(
    df: DataFrame, grouping_column_list: list, date_field_column: Column
) -> DataFrame:
    """
    For a particular column of dates, filter the latest of that date for a select grouping of other columns, returning a full dataset.
    Note that if the provided date_field_column has multiple of the same entries for a grouping_column_list, then this function will return those duplicates.

    :Args:
        df: The DataFrame to be filtered
        grouping_column_list: A list of pyspark.sql.Column variables representing the columns you wish to groupby, i.e. [F.col("column_name")]
        date_field_column: A formatted pyspark.sql.Column of dates

    :Returns:

        latest_date_df: A dataframe with the latest value date_field_column only per grouping

    :Raises:
        TypeError: If any parameter other than the DataFrame does not contain a pyspark.sql.Column
    """

    if isinstance(date_field_column, Column) is False:
        raise TypeError("Column must be of pyspark.sql.Column type")
    for column in grouping_column_list:
        if isinstance(column, Column) is False:
            raise TypeError("List items must be of pyspark.sql.Column type")

    window = Window.partitionBy(grouping_column_list).orderBy(date_field_column.desc())

    latest_date_df = (
        df.withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") == 1)
        .drop(F.col("rank"))
    )

    return latest_date_df


def normalise_column_values(df: DataFrame, col_name: str):
    return df.withColumn(col_name, F.upper(F.regexp_replace(F.col(col_name), " ", "")))


def filter_df_to_maximum_value_in_column(
    df: DataFrame, column_to_filter_on: str
) -> DataFrame:
    max_value = df.agg(F.max(column_to_filter_on)).collect()[0][0]

    return df.filter(F.col(column_to_filter_on) == max_value)


def select_rows_with_value(df: DataFrame, column: str, value_to_keep: str) -> DataFrame:
    """
    Select rows from a DataFrame where the specified column matches the given value.

    Args:
        df (DataFrame): The input DataFrame.
        column (str): The name of the column to filter on.
        value_to_keep (str): The value to keep in the specified column.

    Returns:
        DataFrame: A DataFrame containing only the rows where the specified column matches the given value.
    """
    return df.filter(F.col(column) == value_to_keep)


def select_rows_with_non_null_value(df: DataFrame, column: str) -> DataFrame:
    """
    Select rows from a DataFrame where the specified column has non-null values.

    Args:
        df (DataFrame): The input DataFrame.
        column (str): The name of the column to filter on.

    Returns:
        DataFrame: A DataFrame containing only the rows where the specified column has non-null values.
    """
    return df.filter(F.col(column).isNotNull())
