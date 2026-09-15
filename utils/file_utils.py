import re
from datetime import date

import boto3


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

    The raw bucket's domain names (e.g. domain=ASCWDS) are fixed externally
    and can't be renamed, but the datasets bucket uses the "01_" numbered
    scheme (e.g. domain=01_ascwds) -- so the key's domain segment is renamed
    on the way through.

    Args:
        destination (str): An s3 uri identifying the destination bucket.
        key (str): The source key, whose directory is used in the destination
            path.

    Returns:
        str: The constructed destination s3 uri.
    """
    destination_bucket = split_s3_uri(destination)[0]
    dir_path = get_file_directory(key)
    dir_path = re.sub(
        r"^domain=(\w+)", lambda match: f"domain=01_{match.group(1).lower()}", dir_path
    )
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
