import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)

COMPARE_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date,
]


def calculate_expected_row_count(compare_df: pl.DataFrame) -> int:
    """Derives the expected merged row count from the pre-collapse compare dataset.

    compare_df is job_role_estimates_source at its original row count with all job roles,
    but _01_merge collapses job role estimates down to the published job role
    labels (see merge_utils.collapse_job_role_estimates_to_published_labels) before
    joining, so the merged output has fewer rows per location/import-date group rather
    than compare_df's original.

    Args:
        compare_df (pl.DataFrame): job_role_estimates_source, selected down to
            id_per_locationid_import_date.

    Returns:
        int: distinct location/import-date groups in compare_df, multiplied by the
            number of published job role labels.
    """
    distinct_location_import_dates = compare_df.n_unique(
        subset=[IndCqcColumns.id_per_locationid_import_date]
    )
    return distinct_location_import_dates * len(
        SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
    )


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    The compare dataset is job_role_estimates_source at its pre-collapse grain, so
    the expected row count is derived rather than taken as compare_df's raw row
    count - see calculate_expected_row_count.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=COMPARE_COLS_TO_IMPORT,
    )
    expected_row_count = calculate_expected_row_count(compare_df)

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        ).interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
