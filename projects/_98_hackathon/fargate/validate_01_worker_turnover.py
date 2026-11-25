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
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.hackathon_columns import AgeSource
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import (
    CountryOfBirthGroupLabels,
    EthnicityGroupLabels,
    SourceOfRecruitmentWithinOutsideAscLabels,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.ascwds_labelled_vocab import (
    NationalityGroupLabels,
    TrainingFlagLabels,
)
from utils.column_values.categorical_column_values import (
    CurrentCSSR,
    PrimaryServiceType,
    Region,
)
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
    AWPClean.organisation_id,
    HackCols.main_service_type,
    HackCols.split,
    HackCols.is_leaver,
    HackCols.turnover_rate,
    HackCols.valid_for_analysis,
    HackCols.hours,
    HackCols.hourly_pay,
    HackCols.nlw_hourly_rate,
    HackCols.hourly_pay_minus_nlw,
    HackCols.hourly_pay_minus_nlw_clean,
    HackCols.hourly_pay_diff_to_cssr_median,
    HackCols.age_imputed,
    HackCols.age_source,
    HackCols.nationality_group,
    HackCols.country_of_birth_group,
    ONSClean.current_cssr,
    ONSClean.current_region,
    HackCols.years_in_role,
    HackCols.years_in_sector,
    AWKClean.start_date,
    *NUMERIC_COLUMNS,
    *wUtils.LABEL_COLUMNS,
    *wUtils.GROUP_LABEL_RECODES,
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
    """Validates the train split of the worker turnover data.

    Only the columns the checks need are read, as the dataset is large.

    Args:
        bucket_name (str): the bucket (name only) for the datasets and reports
        source_path (str): the worker turnover train dataset path to validate
        compare_path (str): the cleaned ASC-WDS worker dataset path; its rows kept
            by `filter_worker_turnover_data` and in the train split give the expected row
            count
        reports_path (str): the output path to write reports to
        split (str): the split the source holds, "train"
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}", selected_columns=SOURCE_COLUMNS
    )
    compare_lf = utils.scan_parquet(
        f"s3://{bucket_name}/{compare_path}", selected_columns=vUtils.COMPARE_COLUMNS
    )
    expected_row_count = vUtils.count_expected_rows(compare_lf, split)

    establishment_month = [
        AWKClean.establishment_id,
        AWKClean.ascwds_worker_import_date,
    ]
    is_leaver = pl.col(HackCols.is_leaver)
    turnover_rate = pl.col(HackCols.turnover_rate)
    hourly_pay = pl.col(HackCols.hourly_pay)
    start_date = pl.col(AWKClean.start_date)
    import_date = pl.col(AWKClean.ascwds_worker_import_date)
    age = pl.col(AWKClean.age)
    age_known = age.fill_null(0) > 0
    years_since_start = (import_date - start_date).dt.total_days() / 365.25
    min_age = wUtils.MIN_START_AGE
    max_years = wUtils.MAX_YEARS_SINCE_START_WITHOUT_AGE
    pay_difference = pl.col(HackCols.hourly_pay_minus_nlw)
    pay_difference_clean = pl.col(HackCols.hourly_pay_minus_nlw_clean)
    current_cssr_values = CurrentCSSR(ONSClean.current_cssr).categorical_values
    current_region_values = Region(ONSClean.current_region).categorical_values
    training_flag_labels = f"{AWKClean.training_flag}_labels"

    validation = pb.Validate(
        data=source_df,
        label=f"Validation of {source_path}",
        thresholds=GLOBAL_THRESHOLDS,
        brief=True,
        actions=GLOBAL_ACTIONS,
    )
    validation = vUtils.add_worker_data_checks(validation, split, expected_row_count)
    validation = (
        validation.col_vals_between(
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
        .col_vals_ge(
            HackCols.age_imputed,
            0,
            na_pass=True,
            brief="age_imputed is null or not negative",
        )
        .col_vals_ge(
            HackCols.years_in_role,
            0,
            na_pass=True,
            brief="years_in_role is null or not negative",
        )
        .col_vals_ge(
            HackCols.years_in_sector,
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
            HackCols.valid_for_analysis, brief="valid_for_analysis has no nulls"
        )
        .col_vals_expr(
            expr=pl.col(HackCols.hourly_pay_minus_nlw)
            .is_not_null()
            .eq(hourly_pay.is_not_null()),
            brief="hourly_pay_minus_nlw is populated exactly when hourly_pay is populated",
        )
        .col_vals_expr(
            expr=start_date.is_null() | (start_date <= import_date),
            brief="start date is on or before the import date",
        )
        .col_vals_expr(
            expr=start_date.is_null()
            | ~age_known
            | (age - years_since_start >= min_age),
            brief=f"start date implies a start age of {min_age} or over where age is known",
        )
        .col_vals_expr(
            expr=start_date.is_null()
            | age_known
            | (start_date >= import_date.dt.offset_by(f"-{max_years}y")),
            brief=f"start date is within {max_years} years of the import date where age is unknown",
        )
        .col_vals_expr(
            expr=(pay_difference.is_null() & pay_difference_clean.is_not_null()).not_(),
            brief="hourly_pay_minus_nlw_clean is null whenever hourly_pay_minus_nlw is null",
        )
        .col_vals_ge(
            HackCols.hourly_pay_minus_nlw_clean,
            0,
            na_pass=True,
            brief="hourly_pay_minus_nlw_clean is never negative",
        )
        .col_vals_expr(
            expr=(
                pay_difference_clean.is_not_null()
                & pay_difference_clean.ne(pay_difference)
            )
            .fill_null(False)
            .not_(),
            brief="hourly_pay_minus_nlw_clean equals hourly_pay_minus_nlw when populated",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.hourly_pay_diff_to_cssr_median).is_null()
            | hourly_pay.is_not_null(),
            brief="hourly_pay_diff_to_cssr_median is only populated when hourly_pay is populated",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.years_in_role).is_null()
            | pl.col(AWKClean.start_date).is_not_null(),
            brief="years_in_role is only populated where start_date is populated",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.years_in_sector).is_null()
            | pl.col(AWKClean.start_in_sector).is_not_null(),
            brief="years_in_sector is only populated where start_in_sector is populated",
        )
        .col_vals_in_set(
            HackCols.age_source,
            [
                AgeSource.observed,
                AgeSource.pinned_birth_month,
                AgeSource.assumed_birth_month,
                None,
            ],
            brief="age_source is a known value or null",
        )
        .col_vals_expr(
            expr=pl.col(HackCols.age_imputed)
            .is_not_null()
            .eq(pl.col(HackCols.age_source).is_not_null()),
            brief="age_imputed is populated exactly when age_source is populated",
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
        training_flag_labels: TrainingFlagLabels(
            training_flag_labels, contains_null_values=False
        ),
        HackCols.ethnicity_group_labels: EthnicityGroupLabels(
            HackCols.ethnicity_group_labels, contains_null_values=True
        ),
        HackCols.scerec_wo_group_labels: SourceOfRecruitmentWithinOutsideAscLabels(
            HackCols.scerec_wo_group_labels, contains_null_values=True
        ),
    }

    # One set of steps per column so a report names the column.
    for column, labels in expected_labels.items():
        validation = validation.col_vals_in_set(
            column, [*labels.categorical_values, None]
        )
        validation = validation.specially(
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
        ("--split", "The split the dataset holds, train"),
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
