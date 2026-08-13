import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.filtering_utils import (
    earliest_file_per_month_filter_expr,
    not_null_filter_expr,
    reduced_data_filter_expr,
)
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)

COMPARE_COLS_TO_IMPORT = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
    AWPClean.location_id,
]


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    The compare dataset is the unreduced cleaned ASCWDS workplace data, so the same
    reduction filters applied in _00_prepare are applied here before counting rows -
    otherwise the expected count would include the historical rows and duplicate
    monthly files the prepare step deliberately drops.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")
    compare_df = (
        utils.read_parquet(
            source=f"s3://{bucket_name}/{compare_path}",
            selected_columns=COMPARE_COLS_TO_IMPORT,
        )
        .filter(not_null_filter_expr(column=AWPClean.location_id))
        .filter(
            reduced_data_filter_expr(date_col=AWPClean.ascwds_workplace_import_date)
        )
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWPClean.ascwds_workplace_import_date
            )
        )
    )
    # Each pre-reshape row explodes into one row per published job role label.
    expected_row_count = compare_df.height * len(
        SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
    )

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
        )
        # job role reshape grain
        .rows_distinct(
            columns_subset=[
                AWPClean.establishment_id,
                AWPClean.ascwds_workplace_import_date,
                SLVCols.published_job_role_label,
            ],
            brief="Primary key (establishment_id, ascwds_workplace_import_date, "
            "published_job_role_label) should be unique",
        )
        # categorical
        .col_vals_in_set(
            SLVCols.published_job_role_label,
            SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                SLVCols.published_job_role_label,
                SLVPrepareCategoricalValues.published_job_role_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{SLVCols.published_job_role_label} should have exactly "
            f"{SLVPrepareCategoricalValues.published_job_role_labels_column_values.count_of_categorical_values} distinct values",
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
