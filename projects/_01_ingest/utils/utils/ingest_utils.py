import os
import boto3
import csv
from datetime import date

from typing import List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from utils import utils

TWO_MB = 2000000


def is_csv(filename: str) -> bool:
    """
    Check if the given filename has a .csv extension.

    Args:
        filename (str): The name of the file.

    Returns:
        bool: True if the filename ends with .csv, False otherwise.
    """
    return filename.endswith(".csv")


def get_s3_objects_list(
    bucket_source: str, prefix: str, s3_resource: boto3.resource = None
) -> List[str]:
    """
    Retrieves a list of object keys from an S3 bucket under a given prefix, excluding empty (directory-like) objects.

    Args:
        bucket_source (str): Name of the S3 bucket.
        prefix (str): Prefix to filter objects within the bucket.
        s3_resource (boto3.resource, optional): Pre-initialized Boto3 S3 resource. Defaults to None.

    Returns:
        List[str]: A list of object keys under the specified prefix.
    """
    if s3_resource is None:
        s3_resource = boto3.resource("s3")

    bucket = s3_resource.Bucket(bucket_source)
    object_keys = [
        obj.key for obj in bucket.objects.filter(Prefix=prefix) if obj.size > 0
    ]
    return object_keys


def get_file_directory(filepath: str) -> str:
    """
    Returns the directory portion of a file path.

    Args:
        filepath (str): Full path to the file.

    Returns:
        str: Directory path without the file name.
    """
    return os.path.dirname(filepath)


def construct_s3_uri(bucket_name: str, key: str) -> str:
    """
    Constructs a full S3 URI from a bucket name and key.

    Args:
        bucket_name (str): S3 bucket name.
        key (str): S3 object key or path.

    Returns:
        str: Full S3 URI (e.g., "s3://bucket-name/path/to/object").
    """
    return f"s3://{bucket_name.strip().strip('/')}/{key.lstrip('/')}"


def construct_destination_path(destination: str, key: str) -> str:
    """
    Constructs the destination S3 URI by combining the destination bucket and key directory.

    Args:
        destination (str): Destination S3 URI (bucket or full URI).
        key (str): Source key/path to extract the directory from.

    Returns:
        str: Destination S3 URI pointing to the directory, always ending with '/'.
    """
    destination_bucket, _ = split_s3_uri(destination)
    dir_path = get_file_directory(key).rstrip("/")
    return construct_s3_uri(destination_bucket, f"{dir_path}/")


def read_partial_csv_content(
    bucket: str, key: str, s3_client: boto3.client = None
) -> str:
    """
    Reads a small portion of a CSV file from S3 to use for sampling (e.g., delimiter detection).

    Args:
        bucket (str): S3 bucket name.
        key (str): Key of the CSV object in S3.
        s3_client (boto3.client, optional): Boto3 S3 client. Defaults to None.

    Returns:
        str: Partial content of the CSV file as a UTF-8 string.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    num_bytes = int(response["ContentLength"] * 0.01)
    num_bytes = min(num_bytes, TWO_MB)

    return response["Body"].read(num_bytes).decode("utf-8")


def identify_csv_delimiter(sample_csv: str) -> str:
    """
    Identifies the delimiter used in a sample CSV string.

    Args:
        sample_csv (str): A sample string from a CSV file.

    Returns:
        str: The detected delimiter (e.g., ',' or '|').
    """
    dialect = csv.Sniffer().sniff(sample_csv, [",", "|"])
    return dialect.delimiter


def read_csv(source: str, delimiter: str = ",") -> DataFrame:
    """
    Reads a CSV file using Spark with an optional delimiter.

    Args:
        source (str): Path to the CSV file or directory.
        delimiter (str, optional): CSV delimiter. Defaults to ",".

    Returns:
        DataFrame: DataFrame containing the CSV data.
    """
    spark = utils.get_spark()

    return spark.read.option("delimiter", delimiter).csv(source, header=True)


def read_csv_with_defined_schema(source: str, schema: StructType) -> DataFrame:
    """
    Reads a CSV file using Spark with a predefined schema.

    Args:
        source (str): Path to the CSV file or directory.
        schema (StructType): Spark schema to apply during read.

    Returns:
        DataFrame: DataFrame containing the CSV data.
    """
    spark = utils.get_spark()

    return spark.read.schema(schema).option("header", "true").csv(source)


def generate_s3_datasets_dir_date_path(
    destination_prefix: str,
    domain: str,
    dataset: str,
    date: date,
    version: str = "1.0.0",
) -> str:
    """
    Generates a structured S3 output path based on domain, dataset, and date.

    Args:
        destination_prefix (str): Base path or prefix for output.
        domain (str): Domain name (e.g., 'finance').
        dataset (str): Dataset name (e.g., 'transactions').
        date (date): Date object to use in path formatting.
        version (str, optional): Dataset version string. Defaults to "1.0.0".

    Returns:
        str: Constructed S3 directory path.
    """
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day

    output_dir = f"{destination_prefix}/domain={domain}/dataset={dataset}/version={version}/year={year}/month={month}/day={day}/import_date={import_date}/"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir
