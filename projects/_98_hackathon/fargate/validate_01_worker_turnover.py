import sys

import pointblank as pb
import polars as pl

import projects._98_hackathon.fargate.utils.validation_utils as vUtils
from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.hackathon_columns import (
    HackathonOutputColumns as OutCols,
)

SOURCE_COLUMNS = [
    *vUtils.WORKER_DATA_COLUMNS,
    OutCols.is_leaver,
    OutCols.turnover_rate,
    OutCols.hourly_pay_minus_nlw,
    OutCols.hourly_pay_diff_to_local_authority_median,
    OutCols.years_in_role,
    OutCols.years_in_sector,
]


def main(
    bucket_name: str,
    source_path: str,
    compare_path: str,
    reports_path: str,
    split: str,
) -> None:
    """Validates the train or holdout split of the worker turnover data.

    Only the columns the checks need are read, as the dataset is large.

    Args:
        bucket_name (str): the bucket (name only) for the datasets and reports
        source_path (str): the worker turnover split dataset path to validate
        compare_path (str): the cleaned ASC-WDS worker dataset path; its rows kept
            by `filter_worker_turnover_data` and in the split give the expected row
            count. Unused for train, which holds only rows with
            `valid_for_analysis`, a flag the cleaned data lacks.
        reports_path (str): the output path to write reports to
        split (str): the split the source holds, "train" or "holdout"
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}", selected_columns=SOURCE_COLUMNS
    )
    expected_row_count = None
    if split != "train":
        compare_lf = utils.scan_parquet(
            f"s3://{bucket_name}/{compare_path}",
            selected_columns=vUtils.COMPARE_COLUMNS,
        )
        expected_row_count = vUtils.count_expected_rows(compare_lf, split)

    establishment_month = [OutCols.establishmentid, OutCols.snapshot_date]
    is_leaver = pl.col(OutCols.is_leaver)
    turnover_rate = pl.col(OutCols.turnover_rate)

    validation = pb.Validate(
        data=source_df,
        label=f"Validation of {source_path}",
        thresholds=GLOBAL_THRESHOLDS,
        brief=True,
        actions=GLOBAL_ACTIONS,
    )
    validation = vUtils.add_worker_data_checks(validation, split, expected_row_count)
    if split == "train":
        validation = validation.col_vals_expr(
            expr=pl.col(OutCols.valid_for_analysis).fill_null(False),
            brief="train rows all have valid_for_analysis",
        )
    validation = (
        validation.col_vals_between(
            OutCols.turnover_rate,
            0,
            1,
            na_pass=True,
            brief="turnover_rate is between 0 and 1",
        )
        .col_vals_ge(
            [*vUtils.NUMERIC_COLUMNS, OutCols.hourly_pay],
            0,
            na_pass=True,
            brief="numeric columns and hourly_pay are not negative",
        )
        .col_vals_gt(
            OutCols.hours_worked, 0, na_pass=True, brief="hours is null or above 0"
        )
        .col_vals_gt(
            OutCols.nlw_hourly_rate,
            0,
            brief="nlw_hourly_rate is populated and above 0",
        )
        .col_vals_ge(
            OutCols.age,
            0,
            na_pass=True,
            brief="age is null or not negative",
        )
        .col_vals_ge(
            OutCols.hourly_pay_minus_nlw,
            0,
            na_pass=True,
            brief="hourly_pay_minus_nlw is null or not negative",
        )
        .col_vals_ge(
            OutCols.years_in_role,
            0,
            na_pass=True,
            brief="years_in_role is null or not negative",
        )
        .col_vals_ge(
            OutCols.years_in_sector,
            0,
            na_pass=True,
            brief="years_in_sector is null or not negative",
        )
        # Cross-column checks
        .col_vals_expr(
            expr=turnover_rate.min()
            .over(establishment_month)
            .eq_missing(turnover_rate.max().over(establishment_month)),
            brief="turnover_rate is the same on every row of an establishment-month",
        )
        .col_vals_expr(
            expr=turnover_rate.is_not_null().eq(is_leaver.is_not_null()),
            brief="turnover_rate is populated exactly when is_leaver is populated",
        )
        .col_vals_not_null(
            OutCols.valid_for_analysis, brief="valid_for_analysis has no nulls"
        )
        .col_vals_expr(
            expr=pl.col(OutCols.hourly_pay_minus_nlw)
            .is_not_null()
            .eq(pl.col(OutCols.hourly_pay).is_not_null()),
            brief="hourly_pay_minus_nlw is populated exactly when hourly_pay is populated",
        )
        .col_vals_expr(
            expr=pl.col(OutCols.hourly_pay_diff_to_local_authority_median).is_null()
            | pl.col(OutCols.hourly_pay).is_not_null(),
            brief="hourly_pay_diff_to_local_authority_median is only populated when hourly_pay is populated",
        )
    )

    validation = validation.interrogate()

    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
        ("--split", "The split the dataset holds, train or holdout"),
    )
    print(f"Starting validation for {args.source_path}")

    main(
        args.bucket_name,
        args.source_path,
        args.compare_path,
        args.reports_path,
        args.split,
    )
    print(f"Validation of {args.source_path} complete")
