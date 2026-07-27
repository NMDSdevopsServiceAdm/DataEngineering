import sys

import pointblank as pb

# TEMPORARY - ticket 1820 memory instrumentation. Remove this import and every
# dHelpers.write_checkpoint call below once the measurement has been taken.
import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.diag_helpers as dHelpers
from polars_utils import utils
from polars_utils.filtering_utils import reduced_data_filter_expr
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

COMPARE_COLS_TO_IMPORT = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
]


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    The compare dataset is the unreduced cleaned ASCWDS workplace data, so the same
    reduction filter applied in _00_prepare is applied here before counting rows -
    otherwise the expected count would include the historical rows the prepare step
    deliberately drops.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    dHelpers.write_checkpoint(bucket_name, reports_path, "start")

    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")
    dHelpers.write_checkpoint(
        bucket_name, reports_path, "source_read", row_count=source_df.height
    )

    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=COMPARE_COLS_TO_IMPORT,
    )
    dHelpers.write_checkpoint(
        bucket_name, reports_path, "compare_read", row_count=compare_df.height
    )

    compare_df = compare_df.filter(
        reduced_data_filter_expr(date_col=AWPClean.ascwds_workplace_import_date)
    )
    dHelpers.write_checkpoint(
        bucket_name, reports_path, "compare_filtered", row_count=compare_df.height
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
        ).interrogate()
    )
    dHelpers.write_checkpoint(bucket_name, reports_path, "interrogated")

    vl.write_reports(validation, bucket_name, reports_path)
    dHelpers.write_checkpoint(bucket_name, reports_path, "end")


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
