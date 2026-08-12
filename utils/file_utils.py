import csv
import re
from datetime import date
from typing import Any

import boto3

TWO_MB = 2000000


def split_s3_uri(uri: str) -> tuple[str, str]:
    """
    Converts a given string of an s3 uri into its bucket and key names

    Args:
        uri (str): The s3 uri to be split.

    Returns:
        tuple[str, str]: A tuple of the bucket and key substrings from the s3 uri.
    """
    bucket, prefix = uri.replace("s3://", "").split("/", 1)
    return bucket, prefix


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


def generate_s3_dir(
    destination_prefix: str,
    domain: str,
    dataset: str,
    date: date,
    version: str = "1.0.0",
) -> str:
    """Generates an s3 URI from componant parts of the address and prints the location to stdout (standard output stream).

    Example:
        generate_s3_dir("s3://my-bucket", "my-domain", "my-dataset", date.today(), "1.0.0")
        returns "s3://my-bucket/domain=my-domain/dataset=my-dataset/version=1.0.0/year=YYYY/month=MM/day=DD/import_date=YYYYMMDD/"

    Args:
        destination_prefix(str): The address of the s3 bucket.
        domain(str): The value of the domain key for the URI path.
        dataset(str): The value of the dataset key for the URI path.
        date(date): The date to be used to construct the import_date, year, month, and day partition values for the URI path.
        version(str): The value of the version key for the URI path. Defaults to "1.0.0".

    Returns:
        str: The desired s3 URI
    """
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{destination_prefix}/domain={domain}/dataset={dataset}/version={version}/year={year}/month={month}/day={day}/import_date={import_date}/"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir


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


def list_s3_parquet_import_dates(s3_prefix: str) -> list[int]:
    """
    List import_dates present in a partitioned S3 path.

    Args:
        s3_prefix (str): Base S3 path to the full flattened dataset.

    Returns:
        list[int]: Sorted list of import_date integers.
    """

    match_uri = re.match(r"s3://([^/]+)/(.+)", s3_prefix)
    if not match_uri:
        return []

    bucket = match_uri.group(1)
    prefix = match_uri.group(2).rstrip("/")

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix + "/")

    dates = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = re.search(r"import_date=(\d{8})", key)
            if m:
                date_val = int(m.group(1))
                dates.append(date_val)

    return sorted(dates)


def empty_s3_folder(bucket_name: str, prefix: str) -> None:
    """Empties a folder in a s3 bucket.

    S3 files Keys are full file paths (including the 'folder') so this function uses
    the prefix to determine the contents of a folder and deletes them.

    Example:
        empty_s3_folder("my-bucket", "path/to/my/folder/")

    Args:
        bucket_name (str): the bucket containing the directory to empty
            - cannot be the main dataset bucket
        prefix (str): the path prefix which constitutes the 'folder' to empty
    """
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    to_delete = []
    for item in pages.search("Contents"):
        if item is not None:
            to_delete.append({"Key": item["Key"]})

    if not to_delete:
        print(f"Skipping emptying folder - no objects matching prefix {prefix}")
        return

    keys_str = "\n".join([obj["Key"] for obj in to_delete])
    print(f"Deleting {len(to_delete):} objects:\n{keys_str}")
    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})


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
