import os
import re
import csv

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

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

    def setupSpark(self):
        spark = SparkSession.builder.appName("sfc_data_engineering").getOrCreate()

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
    bucket, prefix = re.search("^s3://([a-zA-Z-_]*)/([a-zA-Z-_/]*)$", path).groups()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    return [
        common_prefix["Prefix"].replace(prefix, "").replace("/", "")
        for common_prefix in response["CommonPrefixes"]
    ]


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


def generate_s3_main_datasets_dir_date_path(domain, dataset, date):
    dir_prepend = "s3://sfc-main-datasets"
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{dir_prepend}/domain={domain}/dataset={dataset}/version=1.0.0/year={year}/month={month}/day={day}/import_date={import_date}"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir


def write_to_parquet(df, output_dir, append=False, partitionKeys=[]):

    if append:
        df.write.mode("append").partitionBy(*partitionKeys).parquet(output_dir)
    else:
        df.write.partitionBy(*partitionKeys).parquet(output_dir)


def read_csv(source, delimiter=","):
    spark = SparkSession.builder.appName(
        "sfc_data_engineering_csv_to_parquet"
    ).getOrCreate()

    df = spark.read.option("delimiter", delimiter).csv(source, header=True)

    return df


def format_date_fields(df, date_column_identifier="date", raw_date_format=None):
    date_columns = [column for column in df.columns if date_column_identifier in column]

    for date_column in date_columns:
        df = df.withColumn(date_column, F.to_date(date_column, raw_date_format))

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


def extract_column_from_schema(schema):
    return [field.name for field in schema.fields]


def extract_col_with_pattern(pattern, schema):
    columns = extract_column_from_schema(schema)
    pattern = re.compile(rf"{pattern}")
    output_columns = []
    for col in columns:
        if pattern.match(col):
            output_columns.append(col)
    return output_columns


def extract_specific_column_types(pattern, schema):
    columns = extract_col_with_pattern(pattern, schema)
    types = []
    for col in columns:
        types.append(col[0:4])
    return types


def format_import_date(df, fieldname="import_date"):
    return df.withColumn(
        fieldname, F.to_date(F.col(fieldname).cast("string"), "yyyyMMdd")
    )


def get_max_snapshot_partitions(location=None):
    if not location:
        return None

    spark = get_spark.spark

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
