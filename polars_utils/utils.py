import argparse
import logging
import uuid
from pathlib import Path

import boto3
import polars as pl
import polars.selectors as cs

util_logger = logging.getLogger(__name__)
util_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
util_logger.addHandler(logging.StreamHandler())
util_logger.handlers[0].setFormatter(formatter)


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
        # Polars may only scan the first hive partition to establish the schema.
        # By including missing_columns="insert", we prevent a failure but will
        # exclude columns introduced in later partitions.
        # TODO: establish full schema from latest data
        raw = (
            pl.scan_parquet(
                source,
                cast_options=pl.ScanCastOptions(
                    missing_struct_fields="insert",
                    extra_struct_fields="ignore",
                ),
                extra_columns="ignore",
                missing_columns="insert",
            )
            .select(selected_columns or cs.all())
            .collect()
        )

    if not exclude_complex_types:
        return raw

    return raw.select(~cs.by_dtype(pl.Struct, pl.List))


def write_to_parquet(
    df: pl.DataFrame,
    output_path: str | Path,
    logger: logging.Logger = util_logger,
    append: bool = True,
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
    df.write_parquet(output_path)
    logger.info("Parquet written to {}".format(output_path))


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
    """Empties a folder in an s3 bucket.

    S3 files Keys are full file paths (including the 'folder') so this function uses
    the prefix to determine which files to delete, rather than a folder.

    Example:
        empty_s3_folder("my-bucket", "path/to/my/folder/")

    Args:
        bucket_name (str): the nucket containing the directory to empty
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
