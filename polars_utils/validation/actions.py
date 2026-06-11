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


def add_list_column_validation_check_flags(
    df: pl.DataFrame, columns: list[str]
) -> pl.DataFrame:
    """
    Adds a new boolean column 'column_validation_passed' indicating whether each list
    in the specified list-type column passes validation based on the following rules:
    - The list value may be null (null values are considered valid) if is
    - Non-null lists must not be empty
    - Non-null lists must not contain any None/null elements

    Additionally, this function adds a second boolean column
    'column_completeness_passed' that indicates whether the original column
    contains no null values (completeness check).

    After creating these validation columns, the original list column is dropped.

    Args:
        df (pl.DataFrame): Input Dataframe with complex columns
        columns (list[str]): The list of list-type columns to validate

    Returns:
        pl.DataFrame: DataFrame with a new bool validation passed column
    """
    expressions = []

    for col in columns:
        validation_expr = (
            (
                pl.col(col).is_null()
                | ((pl.col(col).list.len() > 0) & (~pl.col(col).list.contains(None)))
            )
            .cast(pl.Int64)
            .alias(f"{col}_has_no_empty_or_null")
        )

        completeness_expr = (
            pl.col(col).is_not_null().cast(pl.Int64).alias(f"{col}_is_not_null")
        )

        expressions.extend([validation_expr, completeness_expr])

    df_with_flags = df.with_columns(expressions)

    return df_with_flags.drop(columns)


def make_col_has_fewer_nulls_validator(
    column1: str, column2: str
) -> Callable[[pl.DataFrame], bool]:
    """
    Creates a validation function which checks if column1 has fewer null values than column2.

    This function returns another Callable, for use in pointblank validations,
    particularly `specially` which requires that the inner function accepts only a
    single parameter (pl.DataFrame) as its arguments.

    Args:
        column1 (str): The first column to compare. Expected to have fewer null values.
        column2 (str): The second column to compare. Expected to have more null values than column1.

    Returns:
        Callable[[pl.DataFrame], bool]: The validation function
    """

    def validate_col_has_fewer_nulls(data: pl.DataFrame) -> bool:
        nulls_col1 = data.filter(pl.col(column1).is_null()).height
        nulls_col2 = data.filter(pl.col(column2).is_null()).height
        return nulls_col1 < nulls_col2

    return validate_col_has_fewer_nulls
