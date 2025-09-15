import argparse
import logging
import uuid
from pathlib import Path

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
        # Polars will scan the first hive partition to establish the schema
        # by including missing_columns="insert", we prevent a failure but
        # will exclude columns introduced in later partitions
        # TODO: establish full schema from latest data
        raw = (
            pl.scan_parquet(
                source,
                cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
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
    output_path: str,
    logger: logging.Logger = util_logger,
    append: bool = True,
) -> None:
    """Writes a Polars DataFrame to a Parquet file.

    If the DataFrame is empty, an informational message is logged, and no file
    is written. Otherwise, the DataFrame is written to the specified path,
    optionally partitioned by the given keys.

    Args:
        df (pl.DataFrame): The Polars DataFrame to write.
        output_path (str): The file path where the Parquet file(s) will be written.
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
    else:
        if append:
            output_path += f"{uuid.uuid4()}.parquet"
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


def generate_s3_datasets_dir_date_path(
    destination_prefix,
    domain,
    dataset,
    date,
    version="1.0.0",
):
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{destination_prefix}/domain={domain}/dataset={dataset}/version={version}/year={year}/month={month}/day={day}/import_date={import_date}/"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir
