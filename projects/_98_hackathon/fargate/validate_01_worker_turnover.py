import sys

import pointblank as pb
import polars as pl

import projects._98_hackathon.fargate.utils.validation_utils as vUtils
import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import (
    CountryOfBirthGroupLabels,
    NationalityGroupLabels,
    NotKnownWorkerCodes,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.categorical_column_values import PrimaryServiceType
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)

KEY_COLUMNS = [
    AWKClean.establishment_id,
    AWKClean.worker_id,
    AWKClean.ascwds_worker_import_date,
]

NUMERIC_COLUMNS = [
    column
    for column in wUtils.WORKER_NUMERIC_COLUMNS
    if column not in wUtils.PAY_AND_HOURS_COLUMNS
]

SOURCE_COLUMNS = [
    *KEY_COLUMNS,
    AWKClean.employment_status_clean_labelled,
    AWKClean.main_job_role_clean_labelled,
    HackCols.main_service_type,
    HackCols.split,
    HackCols.establishment_in_both_snapshots,
    HackCols.is_leaver,
    HackCols.months_since_save_start,
    HackCols.months_since_save_end,
    HackCols.establishment_active,
    HackCols.turnover_rate,
    HackCols.headcount_reconciliation_gap,
    HackCols.headcount_gap_within_tolerance,
    HackCols.plausible_turnover,
    HackCols.valid_for_analysis,
    HackCols.hours,
    HackCols.hourly_pay,
    HackCols.nlw_hourly_rate,
    HackCols.hourly_pay_minus_nlw,
    HackCols.nationality_group,
    HackCols.country_of_birth_group,
    *NUMERIC_COLUMNS,
    *wUtils.WORKER_CATEGORICAL_COLUMNS,
    *wUtils.LABEL_COLUMNS,
]

COMPARE_COLUMNS = [
    AWKClean.location_id,
    AWKClean.ascwds_worker_import_date,
    AWKClean.employment_status_clean_labelled,
]


def main(
    bucket_name: str,
    source_path: str,
    compare_path: str,
    reports_path: str,
    split: str,
) -> None:
    """Validates one split of the worker turnover data and writes the reports.

    Only the columns the checks need are read, as the dataset is large.

    Args:
        bucket_name (str): the bucket (name only) for the datasets and reports
        source_path (str): the worker turnover split dataset path to validate
        compare_path (str): the cleaned ASC-WDS worker dataset path; its rows kept
            by `filter_worker_turnover_data` give the expected row count and the
            snapshot months, as a split holds only some of the months
        reports_path (str): the output path to write reports to
        split (str): the split the source holds, "train" or "test"
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}", selected_columns=SOURCE_COLUMNS
    )
    compare_lf = utils.scan_parquet(
        f"s3://{bucket_name}/{compare_path}", selected_columns=COMPARE_COLUMNS
    )
    compare_dates_df = (
        wUtils.filter_worker_turnover_data(compare_lf)
        .select(AWKClean.ascwds_worker_import_date)
        .collect()
    )
    expected_row_count = (
        wUtils.add_split_column(compare_dates_df.lazy())
        .filter(pl.col(HackCols.split) == split)
        .select(pl.len())
        .collect()
        .item()
    )

    import_date = pl.col(AWKClean.ascwds_worker_import_date)
    establishment_in_both_snapshots = pl.col(HackCols.establishment_in_both_snapshots)
    is_leaver = pl.col(HackCols.is_leaver)
    establishment_active = pl.col(HackCols.establishment_active)
    establishment_month = [
        AWKClean.establishment_id,
        AWKClean.ascwds_worker_import_date,
    ]
    turnover_rate = pl.col(HackCols.turnover_rate)
    gap = pl.col(HackCols.headcount_reconciliation_gap)
    gap_within_tolerance = pl.col(HackCols.headcount_gap_within_tolerance)
    plausible_turnover = pl.col(HackCols.plausible_turnover)
    hourly_pay = pl.col(HackCols.hourly_pay)

    snapshot_months = (
        compare_dates_df.select(import_date.dt.truncate("1mo").unique())
        .to_series()
        .to_list()
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
    valid_for_analysis_inputs = (
        establishment_in_both_snapshots
        & establishment_active
        & plausible_turnover
        & gap_within_tolerance
    ).fill_null(False)
    labels_match_codes = pl.all_horizontal(
        pl.col(label).is_not_null().eq(pl.col(code).is_not_null())
        for code, label in zip(wUtils.LABELLED_COLUMNS, wUtils.LABEL_COLUMNS)
    )
    group_matches_code = pl.all_horizontal(
        pl.col(group)
        .is_not_null()
        .eq(pl.col(code).cast(pl.String).replace("", None).is_not_null())
        for code, group in [
            (AWKClean.nationality, HackCols.nationality_group),
            (AWKClean.country_of_birth, HackCols.country_of_birth_group),
        ]
    )
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
        # Row and key checks
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        .col_vals_not_null(KEY_COLUMNS, brief="key columns have no nulls")
        .rows_distinct(
            KEY_COLUMNS,
            brief="establishment, worker and import date are unique",
        )
        .specially(
            vUtils.has_one_import_date_per_month,
            brief="each month has at most one import date",
        )
        .col_vals_ge(
            AWKClean.ascwds_worker_import_date,
            wUtils.EARLIEST_IMPORT_DATE,
            brief=f"import date is on or after {wUtils.EARLIEST_IMPORT_DATE}",
        )
        # Categorical checks
        .col_vals_in_set(HackCols.split, [split], brief=f"split is {split}")
        .col_vals_in_set(
            AWKClean.employment_status_clean_labelled,
            [EmpStat.permanent, EmpStat.temporary],
            brief="employment status is permanent or temporary",
        )
        .col_vals_in_set(
            AWKClean.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
            brief="main job role label is a known value",
        )
        .col_vals_in_set(
            HackCols.main_service_type,
            PrimaryServiceType(HackCols.main_service_type).categorical_values,
            brief="main_service_type is a known value",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.nationality_group)
            .cast(pl.String)
            .is_in(
                NationalityGroupLabels(HackCols.nationality_group).categorical_values
            )
            .fill_null(True),
            brief="nationality_group is null or a known value",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.country_of_birth_group)
            .cast(pl.String)
            .is_in(
                CountryOfBirthGroupLabels(
                    HackCols.country_of_birth_group
                ).categorical_values
            )
            .fill_null(True),
            brief="country_of_birth_group is null or a known value",
        )
        .col_vals_expr(
            expr=pl.all_horizontal(code_is_negative.not_()),
            brief="code raw columns contain no negative codes",
        )
        # Numerical checks
        .col_vals_between(
            HackCols.turnover_rate,
            0,
            1,
            na_pass=True,
            brief="turnover_rate is between 0 and 1",
        )
        .col_vals_ge(
            [*NUMERIC_COLUMNS, HackCols.hourly_pay],
            0,
            na_pass=True,
            brief="numeric raw columns and hourly_pay are not negative",
        )
        .col_vals_gt(HackCols.hours, 0, na_pass=True, brief="hours is null or above 0")
        .col_vals_gt(
            HackCols.nlw_hourly_rate,
            0,
            brief="nlw_hourly_rate is populated and above 0",
        )
        # Cross-column checks
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
        .col_vals_expr(
            expr=turnover_rate.is_not_null().eq(
                establishment_in_both_snapshots.eq_missing(True)
            ),
            brief="turnover_rate is populated exactly when establishment_in_both_snapshots is true",
        )
        .col_vals_expr(
            expr=turnover_rate.min()
            .over(establishment_month)
            .eq_missing(turnover_rate.max().over(establishment_month)),
            brief="turnover_rate is the same on every row of an establishment-month",
        )
        .col_vals_expr(
            expr=gap.is_not_null().eq(establishment_in_both_snapshots.eq_missing(True)),
            brief="headcount_reconciliation_gap is populated exactly when establishment_in_both_snapshots is true",
        )
        .col_vals_expr(
            expr=gap_within_tolerance.eq_missing(
                gap.abs() <= wUtils.DEFAULT_MAX_HEADCOUNT_GAP
            ),
            brief=f"headcount_gap_within_tolerance is true exactly when the absolute gap is at most {wUtils.DEFAULT_MAX_HEADCOUNT_GAP}, null when the gap is null",
        )
        .col_vals_expr(
            expr=plausible_turnover.eq_missing(
                pl.when(turnover_rate.is_not_null()).then(
                    turnover_rate.is_between(0, 1, closed="none")
                )
            ),
            brief="plausible_turnover is true exactly when turnover_rate is strictly between 0 and 1, null when it is null",
        )
        .col_vals_not_null(
            HackCols.valid_for_analysis, brief="valid_for_analysis has no nulls"
        )
        .col_vals_expr(
            expr=pl.col(HackCols.valid_for_analysis).eq(valid_for_analysis_inputs),
            brief="valid_for_analysis is true exactly when its four input flags are all true",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.hourly_pay_minus_nlw)
            .is_not_null()
            .eq(hourly_pay.is_not_null()),
            brief="hourly_pay_minus_nlw is populated exactly when hourly_pay is populated",
        )
        .col_vals_expr(
            expr=labels_match_codes,
            brief="each label column is populated exactly when its code column is",
        )
        .col_vals_expr(
            expr=group_matches_code,
            brief="each country group is populated exactly when its code is non-blank",
        )
    )

    # Categorical checks, one step per column so a report names the column.
    for column, vocabulary in wUtils.LABEL_VOCABULARIES.items():
        validation = validation.col_vals_expr(
            expr=pl.col(column)
            .cast(pl.String)
            .is_in(list(vocabulary.code_to_label()))
            .fill_null(True),
            brief=f"{column} is null or a known code",
        )

    # Nationality and country of birth have no vocabulary; the not known code is nulled.
    for column, not_known_code in [
        (AWKClean.nationality, NotKnownWorkerCodes.nationality),
        (AWKClean.country_of_birth, NotKnownWorkerCodes.country_of_birth),
    ]:
        validation = validation.col_vals_expr(
            expr=pl.col(column).cast(pl.String).ne_missing(not_known_code),
            brief=f"{column} does not hold the not known code",
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
        ("--split", "The split the dataset holds, train or test"),
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
