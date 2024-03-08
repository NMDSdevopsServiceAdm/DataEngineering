import os
import re
import csv
import argparse

from pyspark.sql import SparkSession, DataFrame, Column, Window
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
import pyspark.sql
from typing import List

import boto3

TWO_MB = 2000000


class SetupSpark(object):
    def __init__(self):
        self.spark = None

    def __call__(self):
        if self.spark:
            return self.spark

        self.spark = self.setupSpark()
        return self.spark

    def setupSpark(self) -> SparkSession:
        spark = SparkSession.builder.appName("sfc_data_engineering").getOrCreate()
        spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
        spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY")
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        return spark


get_spark = SetupSpark()


def get_s3_objects_list(bucket_source, prefix, s3_resource=None):
    if s3_resource is None:
        s3_resource = boto3.resource("s3")

    bucket_name = s3_resource.Bucket(bucket_source)
    object_keys = []
    for obj in bucket_name.objects.filter(Prefix=prefix):
        if obj.size > 0:  # Ignore s3 directories
            object_keys.append(obj.key)
    return object_keys


def get_s3_sub_folders_for_path(path, s3_client=None):
    if s3_client is None:
        s3_client = boto3.client("s3")
    bucket, prefix = re.search("^s3://([a-zA-Z-_]*)/([a-zA-Z-=_/]*)$", path).groups()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    return [
        common_prefix["Prefix"].replace(prefix, "").replace("/", "")
        for common_prefix in response["CommonPrefixes"]
    ]


def get_model_name(path_to_model):
    _, prefix = split_s3_uri(path_to_model)
    return prefix.split("/")[1]


def read_partial_csv_content(bucket, key, s3_client=None):
    if s3_client is None:
        s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    num_bytes = int(response["ContentLength"] * 0.01)

    if num_bytes > TWO_MB:
        num_bytes = TWO_MB

    return response["Body"].read(num_bytes).decode("utf-8")


def identify_csv_delimiter(sample_csv):
    dialect = csv.Sniffer().sniff(sample_csv, [",", "|"])
    return dialect.delimiter


def generate_s3_datasets_dir_date_path(destination_prefix, domain, dataset, date):
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{destination_prefix}/domain={domain}/dataset={dataset}/version=1.0.0/year={year}/month={month}/day={day}/import_date={import_date}/"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir


def read_from_parquet(
    data_source: str, selected_columns: List[str] = None
) -> pyspark.sql.DataFrame:
    """
    Reads data from a parquet file and returns a DataFrame with all/selected columns.

    Args:
        data_source: Path to the Parquet file.
        (optional) selected_columns: List of column names to select. Defaults to None (all columns).
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


def read_csv(source, delimiter=","):
    spark = get_spark()

    df = spark.read.option("delimiter", delimiter).csv(source, header=True)

    return df


def read_csv_with_defined_schema(source, schema):
    spark = SparkSession.builder.appName(
        "sfc_data_engineering_spss_csv_to_parquet"
    ).getOrCreate()

    df = spark.read.schema(schema).option("header", "true").csv(source)

    return df


def format_date_fields(df, date_column_identifier="date", raw_date_format=None):
    date_columns = [column for column in df.columns if date_column_identifier in column]

    for date_column in date_columns:
        if "import_date" in date_column:
            continue
        else:
            df = df.withColumn(date_column, F.to_date(date_column, raw_date_format))

    return df


def format_date_string(
    df, new_date_format, date_column_identifier="date", raw_date_format=None
):
    date_columns = [column for column in df.columns if date_column_identifier in column]

    for date_column in date_columns:
        df = df.withColumn(
            date_column,
            F.date_format(F.to_date(date_column, raw_date_format), new_date_format),
        )

    return df


def is_csv(filename):
    return filename.endswith(".csv")


def split_s3_uri(uri):
    bucket, prefix = uri.replace("s3://", "").split("/", 1)
    return bucket, prefix


def construct_s3_uri(bucket_name, key):
    s3 = "s3://"
    trimmed_bucket_name = bucket_name.strip()
    s3_uri = os.path.join(s3, trimmed_bucket_name, key)
    return s3_uri


def get_file_directory(filepath):
    path_delimiter = "/"
    list_dir = filepath.split(path_delimiter)[:-1]
    return path_delimiter.join(list_dir)


def construct_destination_path(destination, key):
    destination_bucket = split_s3_uri(destination)[0]
    dir_path = get_file_directory(key)
    return construct_s3_uri(destination_bucket, dir_path)


def format_import_date(df, fieldname="import_date"):
    return df.withColumn(
        fieldname, F.to_date(F.col(fieldname).cast("string"), "yyyyMMdd")
    )


def create_unix_timestamp_variable_from_date_column(
    df: pyspark.sql.DataFrame, date_col: str, date_format: str, new_col_name: str
) -> pyspark.sql.DataFrame:
    return df.withColumn(
        new_col_name, F.unix_timestamp(F.col(date_col), format=date_format)
    )


def convert_days_to_unix_time(days: int):
    NUMBER_OF_SECONDS_IN_ONE_DAY = 86400
    return days * NUMBER_OF_SECONDS_IN_ONE_DAY


def get_max_snapshot_date(locations_df):
    return locations_df.select(F.max("snapshot_date").alias("max")).first().max


def get_max_snapshot_partitions(location=None):
    if not location:
        return None

    spark = get_spark()

    try:
        previous_snpashots = spark.read.option("basePath", location).parquet(location)
    except AnalysisException:
        return None

    max_year = previous_snpashots.select(F.max("snapshot_year")).first()[0]
    previous_snpashots = previous_snpashots.where(F.col("snapshot_year") == max_year)
    max_month = previous_snpashots.select(F.max("snapshot_month")).first()[0]
    previous_snpashots = previous_snpashots.where(F.col("snapshot_month") == max_month)
    max_day = previous_snpashots.select(F.max("snapshot_day")).first()[0]

    return (f"{max_year}", f"{max_month:0>2}", f"{max_day:0>2}")


def get_latest_partition(df, partition_keys=("run_year", "run_month", "run_day")):
    max_year = df.select(F.max(df[partition_keys[0]])).first()[0]
    df = df.where(df[partition_keys[0]] == max_year)
    max_month = df.select(F.max(df[partition_keys[1]])).first()[0]
    df = df.where(df[partition_keys[1]] == max_month)
    max_day = df.select(F.max(df[partition_keys[2]])).first()[0]
    df = df.where(df[partition_keys[2]] == max_day)
    return df


def collect_arguments(*args):
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

    Args:
        df: The DataFrame to be filtered
        grouping_column_list: A list of pyspark.sql.Column variables representing the columns you wish to groupby, i.e. [F.col("column_name")]
        date_field_column: A formatted pyspark.sql.Column of dates

    Raises:
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
