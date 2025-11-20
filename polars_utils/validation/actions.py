import json
from typing import Callable

import boto3
import pointblank as pb
import polars as pl

from polars_utils import utils


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
    reports_path = reports_path.strip("/")
    utils.empty_s3_folder(bucket_name, reports_path)

    report = validation.get_tabular_report()

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=report.as_raw_html(inline_css=True, make_page=True),
        Bucket=bucket_name,
        Key=f"{reports_path}/summary.html",
    )
    try:
        validation.assert_below_threshold(level="error")
    except AssertionError:
        print("ERROR: Data validation failed. See report for details.")
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
    # get_data_extracts does not provide info for custom validations
    if isinstance(failed_records_df, pl.DataFrame):
        utils.write_to_parquet(
            failed_records_df,  # type: ignore = frame=True returns a df
            f"s3://{bucket_name}/{path}/failed_step_{step_idx}_{assertion}_{columns}.parquet",
            append=False,
        )


def is_unique_count_equal(column: str, value: int) -> Callable[[pl.DataFrame], bool]:
    """Creates a validation function which checks if a the number of different unique
    values in a column matches a provided value.

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

    def inner_callable(df: pl.DataFrame) -> bool:
        return df.n_unique(subset=[column]) == value

    return inner_callable


def list_has_no_empty_or_nulls(column: str) -> Callable[[pl.DataFrame], bool]:
    """
    Creates a validation function that checks whether a list-type column:
      - has no empty lists
      - has no lists containing any None/null values

    This function returns another Callable for use in pointblank validations,
    particularly with `specially`, which requires that the returned function
    accepts only a single parameter (pl.DataFrame).

    Args:
        column (str): The list-type column to check.

    Returns:
        Callable[[pl.DataFrame], bool]: A function that returns True if all
        lists in the column are non-empty and contain no None values.
    """

    def inner_callable(df: pl.DataFrame) -> bool:
        lists = df[column]
        # Condition 1: no empty lists
        no_empty = pl.when(lists.is_not_null()).then(lists.list.len() > 0).otherwise(True)
        # Condition 2: no null elements in any list
        no_nulls = lists.list.contains(None).fill_null(True) == False

        return bool((no_empty & no_nulls).all())

    return inner_callable
