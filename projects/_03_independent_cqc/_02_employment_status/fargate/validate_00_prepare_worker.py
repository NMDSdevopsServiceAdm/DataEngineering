import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.filtering_utils import not_null_filter_expr
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils import (
    RAW_RESHAPED_GROUP_COLUMNS,
    RESHAPED_GROUP_COLUMNS,
    collapse_job_roles_to_published_labels,
)
from projects._03_independent_cqc.utils.filtering_utils import get_matched_ascwds_dates
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)


def main(
    bucket_name: str,
    source_path: str,
    compare_path: str,
    metadata_source: str,
    reports_path: str,
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    _00_prepare_worker aggregates and pivots the cleaned ASCWDS worker data
    down to one row per group in RESHAPED_GROUP_COLUMNS (location,
    establishment, import date and published job role, with a count column
    per employment status), so the prepared dataset is expected to have
    exactly as many rows as there are unique combinations of those columns in
    the cleaned ASCWDS worker data it was built from. The compare dataset is
    the unreduced cleaned data, so the same null-location and
    metadata-matched-dates filters _00_prepare_worker applies are applied here
    first - otherwise the expected count would include rows the prepare step
    deliberately drops. The cleaned data also only has the raw job role label,
    so it's collapsed to the published label here the same way
    _00_prepare_worker does, before counting unique groups.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        metadata_source (str): path to the metadata dataset whose ASCWDS import dates
            have already been CQC-matched, used to select which dates to keep
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")
    matched_dates = get_matched_ascwds_dates(
        metadata_source, IndCQC.ascwds_workplace_import_date
    )
    compare_df = (
        utils.read_parquet(
            source=f"s3://{bucket_name}/{compare_path}",
            selected_columns=RAW_RESHAPED_GROUP_COLUMNS,
        )
        .filter(not_null_filter_expr(column=AWKClean.location_id))
        .filter(
            pl.col(AWKClean.ascwds_worker_import_date).is_in(matched_dates.implode())
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
        # complete columns (pivot fills groups with no workers of a status as 0,
        # so these are never null)
        .col_vals_not_null(
            [
                IndCQC.published_job_role_label,
                EmpStatus.permanent_count,
                EmpStatus.temporary_count,
                EmpStatus.bank_or_pool_count,
                EmpStatus.agency_count,
                EmpStatus.other_count,
            ]
        )
        # numeric
        .col_vals_ge(EmpStatus.permanent_count, 0)
        .col_vals_ge(EmpStatus.temporary_count, 0)
        .col_vals_ge(EmpStatus.bank_or_pool_count, 0)
        .col_vals_ge(EmpStatus.agency_count, 0)
        .col_vals_ge(EmpStatus.other_count, 0)
        # categorical
        .col_vals_in_set(
            IndCQC.published_job_role_label,
            SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                IndCQC.published_job_role_label,
                SLVPrepareCategoricalValues.published_job_role_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.published_job_role_label} should have exactly "
            f"{SLVPrepareCategoricalValues.published_job_role_labels_column_values.count_of_categorical_values} distinct values",
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
        ("--metadata_source", "Source s3 directory for metadata"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(
        args.bucket_name,
        args.source_path,
        args.compare_path,
        args.metadata_source,
        args.reports_path,
    )
    print(f"Validation of {args.source_path} complete")
