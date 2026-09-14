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
from projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_worker_utils import (
    RAW_RESHAPED_GROUP_COLUMNS,
    RESHAPED_GROUP_COLUMNS,
    collapse_job_roles_to_published_labels,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    _00_prepare_worker aggregates and pivots the cleaned ASCWDS worker data
    down to one row per group in RESHAPED_GROUP_COLUMNS (location,
    establishment, import date and published job role, with a count column
    per employment status), so the prepared dataset is expected to have
    exactly as many rows as there are unique combinations of those columns in
    the cleaned ASCWDS worker data it was built from. The compare dataset is
    the unreduced cleaned data, so the same null-location and date-reduction
    filters _00_prepare_worker applies are applied here first - otherwise the
    expected count would include the historical rows and duplicate monthly
    files the prepare step deliberately drops. The cleaned data also only has
    the raw job role label, so it's collapsed to the published label here the
    same way _00_prepare_worker does, before counting unique groups.

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
            selected_columns=RAW_RESHAPED_GROUP_COLUMNS,
        )
        .filter(not_null_filter_expr(column=AWKClean.location_id))
        .filter(reduced_data_filter_expr(date_col=AWKClean.ascwds_worker_import_date))
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWKClean.ascwds_worker_import_date
            )
        )
    )
    compare_df = collapse_job_roles_to_published_labels(compare_df)
    expected_row_count = compare_df.select(RESHAPED_GROUP_COLUMNS).unique().height

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .row_count_match(
            expected_row_count,
            brief=(
                f"Expects {expected_row_count} rows (one per unique group in "
                f"{compare_path})"
            ),
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
