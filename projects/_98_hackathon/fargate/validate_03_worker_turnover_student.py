import sys

import pointblank as pb

import projects._98_hackathon.fargate.utils.validation_utils as vUtils
from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS


def main(
    bucket_name: str,
    source_path: str,
    reports_path: str,
) -> None:
    """Validates the student data: the test split without `is_leaver` and
    `turnover_rate`.

    Only the columns the checks need are read, as the dataset is large.
    `is_leaver` and `turnover_rate` are checked as absent from the file's schema,
    so they are never read.

    Args:
        bucket_name (str): the bucket (name only) for the datasets and reports
        source_path (str): the worker turnover student dataset path to validate
        reports_path (str): the output path to write reports to
    """
    source_uri = f"s3://{bucket_name}/{source_path}"
    source_df = utils.read_parquet(
        source=source_uri, selected_columns=vUtils.WORKER_DATA_COLUMNS
    )
    source_column_names = utils.scan_parquet(source_uri).collect_schema().names()

    validation = pb.Validate(
        data=source_df,
        label=f"Validation of {source_path}",
        thresholds=GLOBAL_THRESHOLDS,
        brief=True,
        actions=GLOBAL_ACTIONS,
    )
    validation = vUtils.add_worker_data_checks(validation)
    validation = validation.specially(
        lambda _: vUtils.has_no_hidden_columns(source_column_names),
        brief="student data has no is_leaver or turnover_rate column",
    ).interrogate()

    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the student dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(
        args.bucket_name,
        args.source_path,
        args.reports_path,
    )
    print(f"Validation of {args.source_path} complete")
