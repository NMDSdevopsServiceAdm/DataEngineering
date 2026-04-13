import argparse
import re
import uuid
from datetime import date
from pathlib import Path

import boto3
import polars as pl
import polars.selectors as cs
from botocore.exceptions import ClientError

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def scan_parquet(
    source: str | Path,
    schema: pl.Schema | dict[str, pl.DataType] | None = None,
    selected_columns: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Reads parquet files into a Polars LazyFrame

    Args:
        source (str | Path): the full path in s3 of the dataset
        schema (pl.Schema | dict[str, pl.DataType] | None, optional): Polars schema
            to apply to dataset read.
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
        print("Determining schema from dataset scan")
        raw = scan_parquet(source, selected_columns=selected_columns).collect()

    if not exclude_complex_types:
        return raw

    return raw.select(~cs.by_dtype(pl.Struct, pl.List))


def write_to_parquet(
    df: pl.DataFrame,
    output_path: str | Path,
    append: bool = True,
    partition_cols: list[str] | None = None,
) -> None:
    """Writes a Polars DataFrame to a Parquet file.

    If the DataFrame is empty, an informational message is printed, and no file is written.
    Otherwise, the DataFrame is written to the specified path,
    optionally partitioned by the given keys.

    Args:
        df (pl.DataFrame): The Polars DataFrame to write.
        output_path (str | Path): The file path where the Parquet file(s) will be written.
            This must be a directory if append is set to True.
        append (bool): Whether to append to existing files or overwrite them. Defaults to True.
        partition_cols (list[str] | None): List of columns to partition by (optional - mutually exclusive with append).

    Return:
        None
    """

    if df.height == 0:
        print("The provided dataframe was empty. No data was written.")
        return

    if append:
        fname = f"{uuid.uuid4()}.parquet"
        if isinstance(output_path, str):
            output_path += fname
        else:
            output_path = output_path / fname

    if not partition_cols:
        df.write_parquet(output_path)
        print("Parquet written to {}".format(output_path))
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
    append: bool = True,
) -> None:
    """
    Sinks a Polars LazyFrame directly to Parquet using sink_parquet (fully lazy), with optional partitioning.

    Args:
        lazy_df (pl.LazyFrame): The Polars LazyFrame to write.
        output_path (str | Path): Directory path to sink Parquet files.
        partition_cols (list[str] | None): Columns to partition by. Defaults to None.
        append (bool): Whether to append (True) or overwrite (False). Defaults to True.

    Returns:
        None: This function does not return any value.

    Raises:
        Exception: If writing the LazyFrame to Parquet fails, e.g., due to file system errors, invalid paths, or issues with the LazyFrame.
    """
    if lazy_df is None or len(lazy_df.collect_schema().names()) == 0:
        print("The provided LazyFrame was empty. No data was written.")
        return

    if append:
        fname = f"{uuid.uuid4()}.parquet"
        if isinstance(output_path, str):
            output_path += fname
        else:
            output_path = output_path / fname

    try:
        if partition_cols:
            pad_cols = [col for col in partition_cols if col in ("day", "month")]

            if pad_cols:
                lazy_df = lazy_df.with_columns(
                    [pl.col(c).cast(pl.Utf8).str.zfill(2) for c in pad_cols]
                )

            path = pl.PartitionBy(
                base_path=f"{output_path}",
                include_key=False,
                key=partition_cols,
            )
            lazy_df.sink_parquet(path=path, mkdir=True, engine="streaming")
            print(
                f"LazyFrame sunk to Parquet at {output_path} partitioned by {partition_cols}"
            )
        else:
            lazy_df.sink_parquet(
                f"{output_path}file.parquet", mkdir=True, engine="streaming"
            )
            print(f"LazyFrame sunk to Parquet at {output_path} without partitioning")
    except Exception as e:
        print(f"ERROR: Failed to sink LazyFrame to Parquet: {e}")
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


def send_sns_notification(
    topic_arn: str,
    subject: str,
    message: str,
    region_name: str = "eu-west-2",
) -> None:
    """Publish an SNS notification based on given data.

    Args:
        topic_arn(str): The ARN for the SNS topic.
        subject(str): The SNS subject line.
        message(str): The SNS message.
        region_name(str): Sets the region for the boto3 client. Defaults to "eu-west-2"

    Raises:
        ClientError: If there is an error writing to SNS
    """
    sns_client = boto3.client("sns", region_name=region_name)
    try:
        sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except ClientError as e:
        print(f"ERROR: {e}")
        print(
            "ERROR: There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN"
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


def filter_to_maximum_value_in_column(
    lf: pl.LazyFrame, column_to_filter: str
) -> pl.LazyFrame:
    """
    Filters a LazyFrame to only include rows where `column_to_filter` has the maximum value.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.
        column_to_filter (str): Name of the column to filter on.

    Returns:
        pl.LazyFrame: Filtered LazyFrame containing only rows with the maximum value.
    """
    max_value: str = "max_value"
    lf = lf.with_columns(pl.col(column_to_filter).max().alias(max_value))
    lf = lf.filter(pl.col(column_to_filter) == pl.col(max_value))
    return lf.drop(max_value)


def coalesce_with_source_labels(cols: list[str], name: str) -> tuple[pl.Expr, pl.Expr]:
    """Return expressions for the coalesced value and its source label.

    Args:
        cols (list[str]): The columns to coalesce from left-to-right.
        name (str): The root name for the new columns.

    Returns:
        tuple[pl.Expr, pl.Expr]: Tuple of expressions:
            - Coalesce expression aliased with `name`.
            - Coalesce source label expression aliased name with suffix "_source".
    """
    val_expr = pl.coalesce(cols).alias(name)

    # Build the label logic
    label_expr = pl.when(pl.col(cols[0]).is_not_null()).then(pl.lit(cols[0]))
    for c in cols[1:]:
        label_expr = label_expr.when(pl.col(c).is_not_null()).then(pl.lit(c))

    return (val_expr, label_expr.alias(f"{name}_source"))


def cast_to_schema(schema: dict[str, pl.DataType]) -> list[pl.Expr]:
    """Cast columns to given schema."""
    return [pl.col(c).cast(dtype) for c, dtype in schema.items()]


def nullify_ct_values_previous_to_first_submission(columns: list) -> list[pl.Expr]:
    """
    Nullifies Capacity Tracker (CT) values for all import dates prior to 2021-5-1.

    This is to ensure that we do not impute filled posts prior to collecting CT
    data.

    Args:
        columns (list): A list of column names to nullify.

    Returns:
        list[pl.Expr]: A list of expressions that null values in given columns when import date
        is prior to 2021-5-1.
    """
    cutoff_condition = pl.col(IndCQC.cqc_location_import_date) < date(2021, 5, 1)

    return [
        pl.when(cutoff_condition).then(None).otherwise(pl.col(col)).alias(col)
        for col in columns
    ]


def _get_selected_value(
    lf: pl.LazyFrame,
    partition_by: list[str],
    order_by: str,
    column_with_null_values: str,
    column_with_data: str,
    new_column: str,
    selection: str,
) -> pl.LazyFrame:
    selection_methods = {"first": pl.first, "last": pl.last}

    if selection not in {"first", "last"}:
        raise ValueError("selection must be 'first' or 'last'")

    method = selection_methods[selection]

    # I have added sort here so it is always in order and if needed we dont
    # need to add extra oderby
    lf = lf.sort(partition_by + [order_by])

    # created a temp column so that we only perform any aggregations/calculation on
    # this new column and original column value is as is
    # not sure if this is the best way but for now kept it in new temp column
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(column_with_data))
        .alias("_valid_value")
    )

    # Do this only for lagged window spec
    lf = lf.with_columns(
        pl.col("_valid_value").shift(1).over(partition_by).alias(new_column)
    )

    # for simple partitionby and orderby window
    # Needs a condition
    lf = lf.with_columns(
        method(
            pl.when(
                pl.col(column_with_null_values).is_not_null(), pl.col(column_with_data)
            )
        ).over(partition_by=partition_by, order_by=order_by)
    )

    # window expression for previous submission
    get_previous_submission_expr = (
        # backward window + last()
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(column_with_data))
        .otherwise(None)
        .forward_fill()
        .over(partition_by)
        .alias(new_column)
    )

    # window expression for next submission
    get_next_submission_expr = (
        # forward window + first()
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(column_with_data))
        .otherwise(None)
        .backward_fill()
        .over(partition_by=partition_by)
        .alias(new_column)
    )

    # Need consition when to call this
    lf = lf.sort([partition_by, column_with_data]).with_columns(
        get_previous_submission_expr,
        get_next_submission_expr,
    )

    return lf


def get_selected_value(
    lf: pl.LazyFrame,
    partition_by: list[str],
    order_by: str,
    column_with_null_values: str,
    column_with_data: str,
    new_column: str,
    selection: str,
    use_dynamic: bool = False,
    dynamic_every: str | None = None,
    dynamic_period: str | None = None,
) -> pl.DataFrame:

    if selection not in {"first", "last"}:
        raise ValueError("selection must be 'first' or 'last'")

    lf = lf.sort(partition_by + [order_by])

    valid_expr = pl.when(pl.col(column_with_null_values).is_not_null()).then(
        pl.col(column_with_data)
    )

    base_expr = (
        valid_expr.forward_fill()
        if selection == "first"
        else valid_expr.backward_fill()
    )

    if not use_dynamic:
        return lf.with_columns(base_expr.over(partition_by).alias(new_column))

    if dynamic_every is None:
        raise ValueError("dynamic_every must be provided when use_dynamic=True")

    grouped = lf.group_by_dynamic(
        index_column=order_by,
        every=dynamic_every,
        period=dynamic_period,
        group_by=partition_by,
        closed="left",
    ).agg(
        (
            pl.col(column_with_data).first()
            if selection == "first"
            else pl.col(column_with_data).last()
        ).alias(new_column)
    )

    return lf.join(grouped, on=partition_by + [order_by], how="left")
