import argparse
import logging
import uuid
from pathlib import Path

import boto3
import polars as pl
import polars.selectors as cs
from botocore.exceptions import ClientError

util_logger = logging.getLogger(__name__)
util_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
util_logger.addHandler(logging.StreamHandler())
util_logger.handlers[0].setFormatter(formatter)


def scan_parquet(
    source: str | Path,
    schema: pl.Schema | None = None,
    selected_columns: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Reads in parquet into a LazyFrame

    Args:
        source (str | Path): the full path in s3 of the dataset
        schema (pl.Schema | None, optional): Polars schema to apply to dataset read
        selected_columns (list[str] | None, optional): list of columns to return as a
            subset of the columns in the schema. Defaults to None.

    Returns:
        pl.LazyFrame: the raw data as a polars LazyFrame

    Raises:
        FileNotFoundError: if there are no files in the source directory

    """
    if isinstance(source, str):
        source = source.strip("/") + "/"

        # Check if directory exists.
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(
            Bucket=source.split("/")[2],
            Prefix=source.split("/", 3)[3],
            MaxKeys=1,
        )
        if "Contents" not in response:
            raise FileNotFoundError(f"No files in {source}")

    lf = pl.scan_parquet(
        source,
        schema=schema,
        cast_options=pl.ScanCastOptions(
            missing_struct_fields="insert",
            extra_struct_fields="ignore",
        ),
        extra_columns="ignore",
        missing_columns="insert",
    ).select(selected_columns or cs.all())

    return lf


def read_parquet(
    source: str | Path,
    schema: pl.Schema | None = None,
    selected_columns: list[str] | None = None,
    exclude_complex_types: bool = False,
) -> pl.DataFrame:
    """Reads in a parquet in a format suitable for validating.

    Args:
        source (str | Path): the full path in s3 of the dataset to be validated
        schema (pl.Schema | None, optional): Polars schema to apply to dataset read
        selected_columns (list[str] | None, optional): list of columns to return as a
            subset of the columns in the schema. Defaults to None.
        exclude_complex_types (bool, optional): whether or not to exclude types which
            cannot be validated using pointblank (ie., Structs, Lists or similar).
            Defaults to False.

    Returns:
        pl.DataFrame: the raw data as a polars Dataframe
    """
    if isinstance(source, str):
        source = source.strip("/") + "/"

    if schema:
        raw = pl.read_parquet(
            source,
            columns=selected_columns,
            schema=schema,
        )
    else:
        logging.info("Determining schema from dataset scan")
        raw = scan_parquet(source, selected_columns=selected_columns).collect()

    if not exclude_complex_types:
        return raw

    return raw.select(~cs.by_dtype(pl.Struct, pl.List))


def write_to_parquet(
    df: pl.DataFrame,
    output_path: str | Path,
    logger: logging.Logger = util_logger,
    append: bool = True,
    partition_cols: list[str] | None = None,
) -> None:
    """Writes a Polars DataFrame to a Parquet file.

    If the DataFrame is empty, an informational message is logged, and no file
    is written. Otherwise, the DataFrame is written to the specified path,
    optionally partitioned by the given keys.

    Args:
        df (pl.DataFrame): The Polars DataFrame to write.
        output_path (str | Path): The file path where the Parquet file(s) will be written.
            This must be a directory if append is set to True.
        logger (logging.Logger): An optional logger instance to use for logging messages.
            If not provided, a default logger will be used (or you can ensure
            `util_logger` is globally available).
        append (bool): Whether to append to existing files or overwrite them. Defaults to False.
        partition_cols (list[str] | None): List of columns to partition by (optional - mutually exclusive with append).

    Return:
        None
    """

    if df.height == 0:
        logger.info("The provided dataframe was empty. No data was written.")
        return

    if append:
        fname = f"{uuid.uuid4()}.parquet"
        if isinstance(output_path, str):
            output_path += fname
        else:
            output_path = output_path / fname

    if not partition_cols:
        df.write_parquet(output_path)
        logger.info("Parquet written to {}".format(output_path))
    else:
        df.rechunk().write_parquet(
            output_path,
            use_pyarrow=True,
            pyarrow_options={"compression": "snappy", "partition_cols": partition_cols},
        )


def sink_to_parquet(
    lazy_df: pl.LazyFrame,
    output_path: str | Path,
    partition_cols: list[str] | None = None,
    logger: logging.Logger = None,
    append: bool = True,
) -> None:
    """
    Sinks a Polars LazyFrame directly to Parquet using sink_parquet (fully lazy), with optional partitioning.

    Args:
        lazy_df (pl.LazyFrame): The Polars LazyFrame to write.
        output_path (str | Path): Directory path to sink Parquet files.
        partition_cols (list[str] | None): Columns to partition by. Defaults to None.
        logger (logging.Logger): Optional logger for messages. Defaults to None.
        append (bool): Whether to append (True) or overwrite (False). Defaults to True.

    Returns:
        None: This function does not return any value.

    Raises:
        Exception: If writing the LazyFrame to Parquet fails, e.g., due to file system errors, invalid paths, or issues with the LazyFrame.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if lazy_df is None or len(lazy_df.collect_schema().names()) == 0:
        logger.info("The provided LazyFrame was empty. No data was written.")
        return

    logger.info("did not finish!")
    output_path = Path(output_path)

    if append:
        fname = f"{uuid.uuid4()}.parquet"
        if isinstance(output_path, str):
            output_path += fname
        else:
            output_path = output_path / fname

    try:
        if partition_cols:
            path = pl.PartitionByKey(
                base_path=f"{output_path}",
                include_key=False,
                by=partition_cols,
            )
            lazy_df.sink_parquet(path=path, mkdir=True, engine="streaming")
            logger.info(
                f"LazyFrame sunk to Parquet at {output_path} partitioned by {partition_cols}"
            )
        else:
            lazy_df.sink_parquet(output_path, engine="streaming")
            logger.info(
                f"LazyFrame sunk to Parquet at {output_path} without partitioning"
            )
    except Exception as e:
        logger.error(f"Failed to sink LazyFrame to Parquet: {e}")
        raise


def get_args(*args: tuple) -> argparse.Namespace:
    """Provides Args from argparse.ArgumentParser for a set of tuples.

    Args:
        *args (tuple): iterable or arguments to unpack and parse, required format for each arg:
            ("--arg_name", "help text", required (bool, default True), default value (optional))

    Raises:
        argparse.ArgumentError: in case of missing, extra, or invalid args

    Returns:
        argparse.Namespace: the parsed args as a Namespace, accessible as attributes
    """
    parser = argparse.ArgumentParser()
    try:
        for arg in args:
            parser.add_argument(
                arg[0],
                help=arg[1],
                required=True if len(arg) < 3 else arg[2],
                default=arg[3] if len(arg) > 3 else None,
            )
        return parser.parse_args()
    except SystemExit:
        parser.print_help()
        raise argparse.ArgumentError(None, "Error parsing argument")


def generate_s3_dir(destination_prefix, domain, dataset, date, version="1.0.0"):
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{destination_prefix}/domain={domain}/dataset={dataset}/version={version}/year={year}/month={month}/day={day}/import_date={import_date}/"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir


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
        logging.info(f"Skipping emptying folder - no objects matching prefix {prefix}")
        return

    keys_str = "\n".join([obj["Key"] for obj in to_delete])
    logging.info(f"Deleting {len(to_delete):} objects:\n{keys_str}")
    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})


def send_sns_notification(
    topic_arn: str,
    subject: str,
    message: str,
    region_name: str = "eu-west-2",
) -> None:
    sns_client = boto3.client("sns", region_name=region_name)
    try:
        sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except ClientError as e:
        util_logger.error(e)
        util_logger.error(
            "There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN"
        )
        raise


def parse_arg_by_type(arg: str) -> bool | int | float | str:
    """
    Converts a given argument into one of boolean, integer, float or string in that order. If conversion fails,
    the string representation of the argument is returned.

    Args:
        arg (str): The argument to be converted.

    Returns:
        bool | int | float | str: The converted argument.
    """
    try:
        stripped = arg.strip()
        if stripped.lower() == "true":
            return True
        elif stripped.lower() == "false":
            return False
        elif "." in stripped:
            return float(stripped)
        elif stripped.isdigit() or stripped[1].isdigit():
            return int(stripped)
        else:
            return str(stripped)
    except (ValueError, TypeError, IndexError):
        return str(arg)


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
