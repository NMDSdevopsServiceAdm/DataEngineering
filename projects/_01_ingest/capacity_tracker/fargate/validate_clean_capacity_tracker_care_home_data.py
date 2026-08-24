import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._01_ingest.capacity_tracker.fargate.utils import (
    clean_capacity_tracker_utils as ctUtils,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)

compare_columns_to_import = [
    CTCH.nurses_employed,
    CTCH.agency_nurses_employed,
    CTCH.care_workers_employed,
    CTCH.agency_care_workers_employed,
    CTCH.non_care_workers_employed,
    CTCH.agency_non_care_workers_employed,
]


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): path to the raw (pre-clean) dataset, to compute the expected row count
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    )

    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
        selected_columns=compare_columns_to_import,
    )
    expected_row_count = compare_df.filter(
        ctUtils.agency_and_non_agency_values_differ_filter()
    ).height

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
            brief=f"Cleaned file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null([CTCHClean.cqc_id, CTCHClean.ct_care_home_import_date])
        # index columns
        .rows_distinct([CTCHClean.cqc_id, CTCHClean.ct_care_home_import_date])
        # numeric column values are between (inclusive)
        .col_vals_between(CTCHClean.nurses_employed, 0, 1000)
        .col_vals_between(CTCHClean.care_workers_employed, 0, 1000)
        .col_vals_between(CTCHClean.non_care_workers_employed, 0, 1000)
        .col_vals_between(CTCHClean.agency_nurses_employed, 0, 1000)
        .col_vals_between(CTCHClean.agency_care_workers_employed, 0, 2500)
        .col_vals_between(CTCHClean.agency_non_care_workers_employed, 0, 1000)
        .col_vals_between(CTCHClean.non_agency_total_employed, 0, 1000)
        .col_vals_between(CTCHClean.agency_total_employed, 0, 4000)
        .col_vals_between(CTCHClean.ct_care_home_total_employed, 1, 4000)
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        (
            "--compare_path",
            "The filepath to the raw dataset to compare against for expected size",
        ),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    print(f"Validation of {args.source_path} complete")
