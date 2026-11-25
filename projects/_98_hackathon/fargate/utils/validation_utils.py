import pointblank as pb
import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils.validation import actions as vl
from utils.column_names.hackathon_columns import HackathonOutputColumns as OutCols
from utils.column_values.ascwds_labelled_vocab import (
    CARE_CERTIFICATE_STATUS,
    DISABLED,
    GENDER,
    SOCIAL_CARE_QUAL_HELD,
    ZERO_HOURS,
    CountryOfBirthGroupLabels,
    EthnicityGroupLabels,
    NationalityGroupLabels,
    SourceOfRecruitmentWithinOutsideAscLabels,
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

KEY_COLUMNS = [OutCols.establishmentid, OutCols.workerid, OutCols.snapshot_date]

NUMERIC_COLUMNS = [
    OutCols.age,
    OutCols.distance_home_work,
    OutCols.sickness_days,
]

# Output label columns and the vocabulary each is checked against.
LABEL_VOCABULARIES = {
    OutCols.gender: GENDER,
    OutCols.disabled: DISABLED,
    OutCols.zerohours_contract: ZERO_HOURS,
    OutCols.has_social_care_qualification_held: SOCIAL_CARE_QUAL_HELD,
    OutCols.care_certificate_status: CARE_CERTIFICATE_STATUS,
}

WORKER_DATA_COLUMNS = [
    *KEY_COLUMNS,
    OutCols.orgid,
    OutCols.job_role,
    OutCols.service_type,
    OutCols.local_authority,
    OutCols.region,
    OutCols.hours_worked,
    OutCols.nationality_group,
    OutCols.country_of_birth_group,
    OutCols.ethnicity_group,
    OutCols.source_of_recruitment_group,
    OutCols.training,
    *NUMERIC_COLUMNS,
    *LABEL_VOCABULARIES,
]


def has_no_hidden_columns(column_names: list[str]) -> bool:
    """Checks `is_leaver` and `turnover_rate` are both absent.

    Args:
        column_names (list[str]): Column names of the student version of the test
            split.

    Returns:
        bool: True if neither hidden column is present.
    """
    return not set(wUtils.STUDENT_HIDDEN_COLUMNS) & set(column_names)


def has_one_snapshot_date_per_month(df: pl.DataFrame) -> bool:
    """Checks each calendar month has at most one snapshot date.

    Args:
        df (pl.DataFrame): Worker turnover data with `snapshot_date`.

    Returns:
        bool: True if no month has more than one distinct snapshot date.
    """
    snapshot_dates = df.get_column(OutCols.snapshot_date)
    return snapshot_dates.n_unique() == snapshot_dates.dt.truncate("1mo").n_unique()


def add_worker_data_checks(validation: pb.Validate) -> pb.Validate:
    """Adds every check on worker turnover data except those on `is_leaver` and
    `turnover_rate`.

    Args:
        validation (pb.Validate): Validation of data holding `WORKER_DATA_COLUMNS`.

    Returns:
        pb.Validate: The validation with the checks added.
    """
    local_authority_values = CurrentCSSR(OutCols.local_authority).categorical_values
    region_values = Region(OutCols.region).categorical_values

    validation = (
        validation
        # Key checks
        .col_vals_not_null(KEY_COLUMNS, brief="key columns have no nulls")
        .col_vals_not_null(OutCols.orgid, brief="orgid has no nulls")
        .rows_distinct(
            KEY_COLUMNS,
            brief="establishment, worker and snapshot date are unique",
        )
        .specially(
            has_one_snapshot_date_per_month,
            brief="each month has at most one snapshot date",
        )
        .col_vals_ge(
            OutCols.snapshot_date,
            wUtils.EARLIEST_IMPORT_DATE,
            brief=f"snapshot date is on or after {wUtils.EARLIEST_IMPORT_DATE}",
        )
        # Categorical checks
        .col_vals_in_set(
            OutCols.job_role,
            CatValues.main_job_role_labels_column_values.categorical_values,
            brief="job role is a known value",
        )
        .col_vals_in_set(
            OutCols.service_type,
            PrimaryServiceType(OutCols.service_type).categorical_values,
            brief="service_type is a known value",
        )
        .col_vals_in_set(
            OutCols.local_authority,
            local_authority_values,
            brief="local_authority is a known value",
        )
        .col_vals_in_set(
            OutCols.region,
            region_values,
            brief="region is a known value",
        )
        # Numerical checks
        .col_vals_ge(
            NUMERIC_COLUMNS,
            0,
            na_pass=True,
            brief="numeric columns are not negative",
        )
        .col_vals_gt(
            OutCols.hours_worked,
            0,
            na_pass=True,
            brief="hours_worked is null or above 0",
        )
    )

    expected_labels = {
        label_column: vocabulary.label_cls(label_column, contains_null_values=True)
        for label_column, vocabulary in LABEL_VOCABULARIES.items()
    } | {
        OutCols.nationality_group: NationalityGroupLabels(
            OutCols.nationality_group, contains_null_values=True
        ),
        OutCols.country_of_birth_group: CountryOfBirthGroupLabels(
            OutCols.country_of_birth_group, contains_null_values=True
        ),
        OutCols.ethnicity_group: EthnicityGroupLabels(
            OutCols.ethnicity_group, contains_null_values=True
        ),
        OutCols.source_of_recruitment_group: SourceOfRecruitmentWithinOutsideAscLabels(
            OutCols.source_of_recruitment_group, contains_null_values=True
        ),
        OutCols.training: TrainingFlagLabels(
            OutCols.training, contains_null_values=False
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
