import re
import sys

import pointblank as pb
import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils
from polars_utils.filtering_utils import (
    earliest_file_per_month_filter_expr,
    reduced_data_filter_expr,
)
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

COMPARE_COLS_TO_IMPORT = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
]

RAW_JOB_ROLE_CODE_COLUMN_PATTERN = re.compile(r"^jr\d+(emp|strt|stop|vacy)$")

KNOWN_JOB_ROLE_LABELS = set(AscwdsWorkerValueLabelsMainjrid.labels_dict.values()) | set(
    pUtils.SYNTHETIC_JOB_ROLE_LABELS.values()
)


def no_leftover_raw_job_role_code_columns(df: pl.DataFrame) -> bool:
    """Checks that no jrNN{suffix}-coded job role columns remain in df.

    Args:
        df (pl.DataFrame): the dataframe to check

    Returns:
        bool: True if no columns match the raw jrNN{suffix} code shape
    """
    return not any(RAW_JOB_ROLE_CODE_COLUMN_PATTERN.match(col) for col in df.columns)


def has_published_job_role_label_columns(df: pl.DataFrame) -> bool:
    """Checks that at least one column is named after a known published job role label.

    Args:
        df (pl.DataFrame): the dataframe to check

    Returns:
        bool: True if at least one column starts with a known job role label
    """
    return any(
        col.startswith(f"{label}_")
        for col in df.columns
        for label in KNOWN_JOB_ROLE_LABELS
    )


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
        .filter(
            reduced_data_filter_expr(date_col=AWPClean.ascwds_workplace_import_date)
        )
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWPClean.ascwds_workplace_import_date
            )
        )
    )
    expected_row_count = compare_df.height

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
        # job role relabelling
        .specially(
            no_leftover_raw_job_role_code_columns,
            brief="No leftover jrNN-coded job role columns should remain after relabelling",
        )
        .specially(
            has_published_job_role_label_columns,
            brief="Job role columns should be present, named after their published labels",
        )
        .interrogate()
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
