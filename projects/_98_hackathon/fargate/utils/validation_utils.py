import pointblank as pb
import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils.validation import actions as vl
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

WORKER_DATA_COLUMNS = [
    *KEY_COLUMNS,
    AWKClean.employment_status_clean_labelled,
    AWKClean.main_job_role_clean_labelled,
    AWPClean.organisation_id,
    HackCols.main_service_type,
    HackCols.split,
    HackCols.valid_for_analysis,
    HackCols.hours,
    HackCols.hourly_pay,
    HackCols.nlw_hourly_rate,
    HackCols.hourly_pay_minus_nlw,
    HackCols.age_imputed,
    HackCols.age_source,
    HackCols.nationality_group,
    HackCols.country_of_birth_group,
    ONSClean.current_cssr,
    ONSClean.current_region,
    *NUMERIC_COLUMNS,
    *wUtils.LABEL_COLUMNS,
]


COMPARE_COLUMNS = [
    AWKClean.location_id,
    AWKClean.ascwds_worker_import_date,
    AWKClean.employment_status_clean_labelled,
]


def count_expected_rows(compare_lf: pl.LazyFrame, split: str) -> int:
    """Counts the rows the worker turnover job should write to a split.

    Args:
        compare_lf (pl.LazyFrame): Cleaned ASC-WDS worker data with `COMPARE_COLUMNS`.
        split (str): The split to count, "train", "holdout" or "test".

    Returns:
        int: Rows kept by `filter_worker_turnover_data` that fall in the split.
    """
    compare_dates_df = (
        wUtils.filter_worker_turnover_data(compare_lf)
        .select(AWKClean.ascwds_worker_import_date)
        .collect()
    )
    return (
        wUtils.add_split_column(compare_dates_df.lazy())
        .filter(pl.col(HackCols.split) == split)
        .select(pl.len())
        .collect()
        .item()
    )


def has_no_hidden_columns(column_names: list[str]) -> bool:
    """Checks `is_leaver` and `turnover_rate` are both absent.

    Args:
        column_names (list[str]): Column names of the student version of the test
            split.

    Returns:
        bool: True if neither hidden column is present.
    """
    return not set(wUtils.STUDENT_HIDDEN_COLUMNS) & set(column_names)


def has_one_import_date_per_month(df: pl.DataFrame) -> bool:
    """Checks each calendar month has at most one import date.

    Args:
        df (pl.DataFrame): Worker turnover data with `ascwds_worker_import_date`.

    Returns:
        bool: True if no month has more than one distinct import date.
    """
    import_dates = df.get_column(AWKClean.ascwds_worker_import_date)
    return import_dates.n_unique() == import_dates.dt.truncate("1mo").n_unique()


def add_worker_data_checks(
    validation: pb.Validate, split: str, expected_row_count: int
) -> pb.Validate:
    """Adds every check on worker turnover data except those on `is_leaver` and
    `turnover_rate`.

    Args:
        validation (pb.Validate): Validation of data holding `WORKER_DATA_COLUMNS`.
        split (str): The split the data holds, "train", "holdout" or "test".
        expected_row_count (int): The rows the split should hold.

    Returns:
        pb.Validate: The validation with the checks added.
    """
    hourly_pay = pl.col(HackCols.hourly_pay)
    current_cssr_values = CurrentCSSR(ONSClean.current_cssr).categorical_values
    current_region_values = Region(ONSClean.current_region).categorical_values
    training_flag_labels = f"{AWKClean.training_flag}_labels"

    validation = (
        validation
        # Row and key checks
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        .col_vals_not_null(KEY_COLUMNS, brief="key columns have no nulls")
        .col_vals_not_null(
            AWPClean.organisation_id, brief="organisation_id has no nulls"
        )
        .rows_distinct(
            KEY_COLUMNS,
            brief="establishment, worker and import date are unique",
        )
        .specially(
            has_one_import_date_per_month,
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
        .col_vals_in_set(
            ONSClean.current_cssr,
            current_cssr_values,
            brief="current_cssr is a known value",
        )
        .col_vals_in_set(
            ONSClean.current_region,
            current_region_values,
            brief="current_region is a known value",
        )
        # Numerical checks
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
        # Cross-column checks
        .col_vals_not_null(
            HackCols.valid_for_analysis, brief="valid_for_analysis has no nulls"
        )
        .col_vals_expr(
            expr=pl.col(HackCols.hourly_pay_minus_nlw)
            .is_not_null()
            .eq(hourly_pay.is_not_null()),
            brief="hourly_pay_minus_nlw is populated exactly when hourly_pay is populated",
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

    return validation
