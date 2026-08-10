import csv
from typing import Any

import boto3

from utils.utils import split_s3_uri

TWO_MB = 2000000


def is_csv(filename: str) -> bool:
    """
    Checks whether a filename has a .csv extension.

    Args:
        filename (str): The filename or path to check.

    Returns:
        bool: True if the filename ends with ".csv", False otherwise.
    """
    return filename.endswith(".csv")


def identify_csv_delimiter(sample_csv: str) -> str:
    """
    Identifies the delimiter used in a sample of CSV content.

    Args:
        sample_csv (str): A sample of CSV file content.

    Returns:
        str: The identified delimiter, either "," or "|".
    """
    dialect = csv.Sniffer().sniff(sample_csv, [",", "|"])
    return dialect.delimiter


def read_partial_csv_content(bucket: str, key: str, s3_client: Any = None) -> str:
    """
    Reads a sample of a CSV file's content from S3, capped at TWO_MB.

    Reads 1% of the file's total size, or TWO_MB, whichever is smaller - enough
    to sample the file's delimiter and structure without downloading it in full.

    Args:
        bucket (str): The S3 bucket containing the file.
        key (str): The S3 key of the file.
        s3_client (Any, optional): A boto3 S3 client. Defaults to None, in which
            case one is created.

    Returns:
        str: The decoded partial content of the file.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    num_bytes = int(response["ContentLength"] * 0.01)

    if num_bytes > TWO_MB:
        num_bytes = TWO_MB

    return response["Body"].read(num_bytes).decode("utf-8")


def get_s3_objects_list(
    bucket_source: str, prefix: str, s3_resource: Any = None
) -> list[str]:
    """
    Lists the keys of all objects (excluding directories) under an S3 prefix.

    Args:
        bucket_source (str): The S3 bucket to list objects from.
        prefix (str): The prefix to filter objects by.
        s3_resource (Any, optional): A boto3 S3 resource. Defaults to None, in
            which case one is created.

    Returns:
        list[str]: The keys of all matching objects.
    """
    if s3_resource is None:
        s3_resource = boto3.resource("s3")

    bucket_name = s3_resource.Bucket(bucket_source)
    object_keys = []
    for obj in bucket_name.objects.filter(Prefix=prefix):
        if obj.size > 0:  # Ignore s3 directories
            object_keys.append(obj.key)
    return object_keys


def get_file_directory(filepath: str) -> str:
    """
    Returns the directory portion of a filepath, excluding the filename.

    Args:
        filepath (str): The filepath to split.

    Returns:
        str: The directory portion of the filepath, or an empty string if
            filepath contains no "/".
    """
    path_delimiter = "/"
    list_dir = filepath.split(path_delimiter)[:-1]
    return path_delimiter.join(list_dir)


def construct_s3_uri(bucket_name: str, key: str) -> str:
    """
    Constructs an s3 uri from a bucket name and key.

    Args:
        bucket_name (str): The name of the s3 bucket.
        key (str): The key (path) within the bucket.

    Returns:
        str: The constructed s3 uri.
    """
    trimmed_bucket_name = bucket_name.strip()
    return f"s3://{trimmed_bucket_name}/{key}"


def construct_destination_path(destination: str, key: str) -> str:
    """
    Constructs a destination s3 uri, combining the destination bucket with the
    directory of the given key.

    Args:
        destination (str): An s3 uri identifying the destination bucket.
        key (str): The source key, whose directory is used in the destination
            path.

    Returns:
        str: The constructed destination s3 uri.
    """
    destination_bucket = split_s3_uri(destination)[0]
    dir_path = get_file_directory(key)
    return construct_s3_uri(destination_bucket, dir_path)
