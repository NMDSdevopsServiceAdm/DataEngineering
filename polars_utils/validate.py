import json
from pathlib import Path
from typing import Callable

import boto3
import pointblank as pb
import polars as pl
import polars.selectors as cs

from polars_utils import utils
from polars_utils.logger import get_logger

logger = get_logger(__name__)


def read_parquet(
    source: str | Path,
    selected_columns: list[str] | None = None,
    exclude_complex_types: bool = False,
) -> pl.DataFrame:
    """Reads in a parquet in a format suitable for validating.

    Args:
        source (str | Path): the full path in s3 of the dataset to be validated
        selected_columns (list[str] | None, optional): list of columns to return as a 
            subset of the columns in the schema. Defaults to None.
        exclude_complex_types (bool, optional): whether or not to exclude types which 
            cannot be validated using pointblank (ie., Structs, Lists or similar). 
            Defaults to False.

    Returns:
        pl.DataFrame: the raw data as a polars Dataframe
    """
    raw = pl.scan_parquet(
        source,
        cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        # extra_columns="ignore",
    ).select(selected_columns or cs.all())

    if not exclude_complex_types:
        return raw.collect()

    return raw.select(~cs.by_dtype(pl.Struct, pl.List)).collect()


def write_reports(validation: pb.Validate, bucket_name: str, reports_path: str) -> None:
    """Writes the reports for a given `pb.Validate` object to s3, including:
        - summary report as an HTML `pb.GT`
        - a pl.Dataframe for each failed step (if any) including every failing record

    Args:
        validation (pb.Validate): the result of interrogating the defined validation
        bucket_name (str): the bucket to save reports to
            - shoud correspond to workspace / feature branch name
        reports_path (str): the filepath for the reports

    Raises:
        AssertionError: in case of the dataset failing the validation rules
    """

    report = validation.get_tabular_report()

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=report.as_raw_html(inline_css=True, make_page=True),
        Bucket=bucket_name,
        Key=f"{reports_path}/index.html",
    )
    try:
        validation.assert_below_threshold(level="warning")
    except AssertionError:
        logger.error("Data validation failed. See report for details.")
        steps = json.loads(validation.get_json_report())
        # JSON report includes a detailed list of each validation step, including failures
        # Note that some 'steps' result in several steps in the execution
        # eg. a null check over several columns
        for step in steps:
            _report_on_fail(step, validation, bucket_name, reports_path)
        raise  # ensures that the task fails if any warnings / errors


def _report_on_fail(
    step: dict, validation: pb.Validate, bucket_name: str, path: str
) -> None:
    """Checks a given pb.Validate step for failures and writes the failed records to 
    S3 if present.

    Args:
        step (dict): metadata on the validation step result
        validation (pb.Validate): the Validate object containing the resulting data
        bucket_name (str): the bucket in which to write reports
        path (str): the filepath in the bucket to write, should include the validation 
            report name
    """
    if step["all_passed"]:
        return

    step_idx = step["i"]
    assertion = step["assertion_type"]
    _col_or_cols = step["column"]  # could be a string or a list
    columns = "_".join(_col_or_cols) if isinstance(_col_or_cols, list) else _col_or_cols

    failed_records_df = validation.get_data_extracts(step_idx, frame=True)
    utils.write_to_parquet(
        failed_records_df,  # type: ignore = frame=True returns a df
        f"s3://{bucket_name}/{path}/failed_step_{step_idx}_{assertion}_{columns}.parquet",
    )


def is_unique_count_equal(column: str, value: int) -> Callable[[pl.DataFrame], bool]:
    """Creates a function which checks if a the number of different unique values in 
    a column matches a provided value.

    This function returns another Callable, for use in pointblank validations, 
    particularly `specially` which requires that the inner function accepts only a 
    single parameter (pl.DataFrame) as its arguments.

    Args:
        column (str): the column to check
        value (int): the value to assert against

    Returns:
        Callable[[pl.DataFrame], bool]: the inner function which checks the unique 
        value count
    """

    def is_unique_count_equal(df: pl.DataFrame) -> bool:
        return df.n_unique(subset=[column]) == value

    return is_unique_count_equal
