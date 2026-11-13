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
    HackCols.is_leaver,
    HackCols.turnover_rate,
    HackCols.valid_for_analysis,
    HackCols.hours,
    HackCols.hourly_pay,
    HackCols.nlw_hourly_rate,
    HackCols.hourly_pay_minus_nlw,
    HackCols.nationality_group,
    HackCols.country_of_birth_group,
    *NUMERIC_COLUMNS,
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
            by `filter_worker_turnover_data` and in the split give the expected row
            count
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

    establishment_month = [
        AWKClean.establishment_id,
        AWKClean.ascwds_worker_import_date,
    ]
    is_leaver = pl.col(HackCols.is_leaver)
    turnover_rate = pl.col(HackCols.turnover_rate)
    hourly_pay = pl.col(HackCols.hourly_pay)

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
            HackCols.valid_for_analysis, brief="valid_for_analysis has no nulls"
        )
        .col_vals_expr(
            expr=pl.col(HackCols.hourly_pay_minus_nlw)
            .is_not_null()
            .eq(hourly_pay.is_not_null()),
            brief="hourly_pay_minus_nlw is populated exactly when hourly_pay is populated",
        )
    )

    expected_labels = {
        label_column: vocabulary.label_cls(label_column, contains_null_values=True)
        for label_column, vocabulary in zip(
            wUtils.LABEL_COLUMNS, wUtils.LABEL_VOCABULARIES.values()
        )
    } | {
        HackCols.nationality_group: NationalityGroupLabels(
            HackCols.nationality_group, contains_null_values=True
        ),
        HackCols.country_of_birth_group: CountryOfBirthGroupLabels(
            HackCols.country_of_birth_group, contains_null_values=True
        ),
    }

    # One pair of steps per column so a report names the column.
    for column, labels in expected_labels.items():
        validation = validation.col_vals_in_set(
            column, [*labels.categorical_values, None]
        ).specially(
            vl.is_unique_count_equal(column, labels.count_of_categorical_values),
            brief=f"{column} needs to be one of {labels.categorical_values} or null",
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
