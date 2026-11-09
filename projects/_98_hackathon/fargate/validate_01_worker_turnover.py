import sys

import pointblank as pb
import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.categorical_column_values import PrimaryServiceType

KEY_COLUMNS = [
    AWKClean.establishment_id,
    AWKClean.worker_id,
    AWKClean.ascwds_worker_import_date,
]

SOURCE_COLUMNS = [
    *KEY_COLUMNS,
    AWKClean.employment_status_clean_labelled,
    HackCols.main_service_type,
    HackCols.establishment_in_both_snapshots,
    HackCols.is_leaver,
    HackCols.months_since_save_start,
    HackCols.months_since_save_end,
    HackCols.establishment_active,
    *wUtils.WORKER_NUMERIC_COLUMNS,
    *wUtils.WORKER_CATEGORICAL_COLUMNS,
]

COMPARE_COLUMNS = [
    AWKClean.location_id,
    AWKClean.ascwds_worker_import_date,
    AWKClean.employment_status_clean_labelled,
]


def has_at_most_one_end_month_per_start_month(df: pl.DataFrame) -> bool:
    """Checks that no month 12 months after a snapshot month has several snapshots.

    A start month's end month is 12 months later, so each end month must hold
    at most one distinct import date. Takes a `pl.DataFrame` as `specially`
    passes the whole dataset.

    Args:
        df (pl.DataFrame): Worker turnover data with `ascwds_worker_import_date`.

    Returns:
        bool: True if every end month has at most one distinct import date.
    """
    snapshot_months = (
        df.get_column(AWKClean.ascwds_worker_import_date).unique().dt.truncate("1mo")
    )
    end_months = snapshot_months.filter(
        snapshot_months.is_in(snapshot_months.dt.offset_by("12mo").to_list())
    )
    return not end_months.is_duplicated().any()


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates the flagged worker turnover data and writes a summary report and
    the failing records.

    Only the columns the checks need are read, as the dataset is large.

    Args:
        bucket_name (str): the bucket (name only) in which to source the datasets
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the flagged worker turnover dataset path to validate
        compare_path (str): the cleaned ASC-WDS worker dataset path. Its rows kept
            by `filter_worker_turnover_data` give the expected row count; only the
            filter's columns are scanned.
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}", selected_columns=SOURCE_COLUMNS
    )
    compare_lf = utils.scan_parquet(
        f"s3://{bucket_name}/{compare_path}", selected_columns=COMPARE_COLUMNS
    )
    expected_row_count = (
        wUtils.filter_worker_turnover_data(compare_lf).select(pl.len()).collect().item()
    )

    import_date = pl.col(AWKClean.ascwds_worker_import_date)
    establishment_in_both_snapshots = pl.col(HackCols.establishment_in_both_snapshots)
    is_leaver = pl.col(HackCols.is_leaver)
    establishment_active = pl.col(HackCols.establishment_active)

    snapshot_months = (
        source_df.select(import_date.dt.truncate("1mo").unique()).to_series().to_list()
    )
    has_end_snapshot = (
        import_date.dt.truncate("1mo").dt.offset_by("12mo").is_in(snapshot_months)
    )
    gaps_within_limit = (
        (
            pl.col(HackCols.months_since_save_start)
            <= wUtils.DEFAULT_MAX_MONTHS_SINCE_SAVE
        )
        & (
            pl.col(HackCols.months_since_save_end)
            <= wUtils.DEFAULT_MAX_MONTHS_SINCE_SAVE
        )
    ).fill_null(False)
    code_is_negative = (
        pl.col(wUtils.WORKER_CATEGORICAL_COLUMNS)
        .cast(pl.String)
        .str.starts_with("-")
        .fill_null(False)
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
        # columns and keys
        .col_exists(SOURCE_COLUMNS, brief="expected flag and raw columns exist")
        .col_vals_not_null(KEY_COLUMNS, brief="key columns have no nulls")
        .rows_distinct(
            KEY_COLUMNS,
            brief="establishment, worker and import date are unique",
        )
        .specially(
            has_at_most_one_end_month_per_start_month,
            brief="each start month has at most one end month, 12 months later",
        )
        # filters applied by the job
        .col_vals_ge(
            AWKClean.ascwds_worker_import_date,
            wUtils.EARLIEST_IMPORT_DATE,
            brief=f"import date is on or after {wUtils.EARLIEST_IMPORT_DATE}",
        )
        .col_vals_in_set(
            AWKClean.employment_status_clean_labelled,
            [EmpStat.permanent, EmpStat.temporary],
            brief="employment status is permanent or temporary",
        )
        # main service type
        .col_vals_in_set(
            HackCols.main_service_type,
            PrimaryServiceType(HackCols.main_service_type).categorical_values,
            brief="main_service_type is a known value",
        )
        # flags
        .col_vals_expr(
            expr=is_leaver.is_not_null().eq(
                establishment_in_both_snapshots.eq_missing(True)
            ),
            brief="is_leaver is populated exactly when establishment_in_both_snapshots is true",
        )
        .col_vals_expr(
            expr=establishment_in_both_snapshots.is_not_null().eq(has_end_snapshot),
            brief="establishment_in_both_snapshots is populated exactly when a snapshot exists 12 months later",
        )
        .col_vals_expr(
            expr=establishment_active.is_not_null().eq(is_leaver.is_not_null()),
            brief="establishment_active is populated exactly when is_leaver is populated",
        )
        .col_vals_expr(
            expr=establishment_active.ne_missing(True) | gaps_within_limit,
            brief="establishment_active is true only when both save gaps are within the limit",
        )
        # raw worker columns
        .col_vals_ge(
            wUtils.WORKER_NUMERIC_COLUMNS,
            0,
            na_pass=True,
            brief="numeric raw columns are not negative",
        )
        .col_vals_expr(
            expr=pl.all_horizontal(code_is_negative.not_()),
            brief="code raw columns contain no negative codes",
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
