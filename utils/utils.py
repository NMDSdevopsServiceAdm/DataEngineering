import argparse
from typing import Any, Generator, List, Optional

import pydeequ
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


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
            .config("spark.jars.packages", pydeequ.deequ_maven_coord)
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .getOrCreate()
        )
        spark.sql("set spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY")
        spark.sql("set spark.sql.parquet.datetimeRebaseModeInRead=LEGACY")
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        return spark


get_spark = SetupSpark()


def read_from_parquet(
    data_source: str,
    selected_columns: List[str] = None,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Reads data from a parquet file and returns a DataFrame with all/selected columns.

    Args:
        data_source (str): Path to the Parquet file.
        selected_columns (List[str]): Optional - List of column names to select. Defaults to None (all columns).
        schema (Optional[StructType]): Optional - schema to use when reading parquet. Defaults to None.

    Returns:
        DataFrame: A dataframe of the data in the parquet file, with all or selected columns.
    """
    spark_session = get_spark()
    print(f"Reading data from {data_source}")

    if schema:
        df = spark_session.read.schema(schema).parquet(data_source)
    else:
        df = spark_session.read.parquet(data_source)

    if selected_columns:
        df = df.select(selected_columns)

    return df


def write_to_parquet(
    df: DataFrame, output_dir: str, mode: str = None, partitionKeys=[]
):
    # A refactored version of this function, using Polars rather than PySpark, is available in polars_utils/utils.py
    df.write.mode(mode).partitionBy(*partitionKeys).parquet(output_dir)


def read_csv_with_defined_schema(source, schema):
    spark = get_spark()

    df = spark.read.schema(schema).option("header", "true").csv(source)

    return df


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
    # A refactored version of this function, using Polars rather than PySpark, is available in polars_utils/utils.py
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(
            arg[0],
            help=arg[1],
            required=True,
        )

    parsed_args, _ = parser.parse_known_args()

    return (vars(parsed_args)[arg[0][2:]] for arg in args)


# converted to polars -> polars_utils.utils.filter_to_maximum_value_in_column
def filter_df_to_maximum_value_in_column(
    df: DataFrame, column_to_filter_on: str
) -> DataFrame:
    max_value = df.agg(F.max(column_to_filter_on)).collect()[0][0]

    return df.filter(F.col(column_to_filter_on) == max_value)
