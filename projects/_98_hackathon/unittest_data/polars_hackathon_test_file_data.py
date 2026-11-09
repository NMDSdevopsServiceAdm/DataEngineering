from dataclasses import dataclass
from datetime import date
from typing import Any

from projects._98_hackathon.fargate.utils.worker_turnover_utils import (
    WORKER_CATEGORICAL_COLUMNS,
    WORKER_NUMERIC_COLUMNS,
    WORKER_DATE_COLUMNS,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.ascwds_labelled_vocab import (
    APPRENTICE,
    CARE_CERTIFICATE_STATUS,
    COUNTRY_OF_BIRTH_CODE_TO_LABEL,
    DISABLED,
    ETHNICITY,
    GENDER,
    HOURLY_OR_ANNUAL_PAY,
    LEVEL_SOCIAL_CARE_QUALIFICATION_HELD,
    NATIONALITY_CODE_TO_LABEL,
    SOCIAL_CARE_QUAL_HELD,
    SOURCE_OF_RECRUITMENT,
    ZERO_HOURS,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.ascwds_labelled_vocab import MainServiceID

START = date(2021, 1, 5)
END = date(2022, 1, 5)  # 12 months after START

WORKER_LABELS_DICT = {
    AWKClean.gender: GENDER.code_to_label(),
    AWKClean.ethnicity: ETHNICITY.code_to_label(),
    AWKClean.nationality: NATIONALITY_CODE_TO_LABEL,
    AWKClean.country_of_birth: COUNTRY_OF_BIRTH_CODE_TO_LABEL,
    AWKClean.disabled: DISABLED.code_to_label(),
    AWKClean.hourly_or_annual_pay: HOURLY_OR_ANNUAL_PAY.code_to_label(),
    AWKClean.social_care_qual_held: SOCIAL_CARE_QUAL_HELD.code_to_label(),
    AWKClean.level_social_care_qualification_held: (
        LEVEL_SOCIAL_CARE_QUALIFICATION_HELD.code_to_label()
    ),
    AWKClean.care_certificate_status: CARE_CERTIFICATE_STATUS.code_to_label(),
    AWKClean.apprentice: APPRENTICE.code_to_label(),
    AWKClean.zero_hours: ZERO_HOURS.code_to_label(),
    AWKClean.source_of_recruitment: SOURCE_OF_RECRUITMENT.code_to_label(),
}


def coded_worker_row(worker_id: str, **codes: str) -> tuple[str | None, ...]:
    """Returns a (worker_id, *coded columns) row with every code null unless set."""
    return (
        worker_id,
        *[codes.get(column) for column in WORKER_CATEGORICAL_COLUMNS],
    )


def raw_worker_row(
    worker_id: str,
    import_date: str,
    establishment_id: str = "1-001",
    **values: str,
) -> dict[str, str | None]:
    """Returns a raw worker row with every modelling column null unless set."""
    modelling_columns = [
        *WORKER_CATEGORICAL_COLUMNS,
        *WORKER_NUMERIC_COLUMNS,
        *WORKER_DATE_COLUMNS,
    ]
    return {
        AWKClean.establishment_id: establishment_id,
        AWKClean.worker_id: worker_id,
        AWKClean.import_date: import_date,
        **{column: None for column in modelling_columns},
        **values,
    }


@dataclass
class FilterWorkerTurnoverDataCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


@dataclass
class AddIsLeaverFlagCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


@dataclass
class AddMainServiceTypeCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


@dataclass
class GetLatestSaveDatePerEstablishmentCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]
    snapshot_rows: list[tuple[Any, ...]] | None = None


@dataclass
class AddEstablishmentActiveFlagCase:
    id: str
    worker_rows: list[tuple[Any, ...]]
    save_date_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]
    n_months: int = 6


@dataclass
class WorkerTurnoverData:
    add_main_service_type_cases = [
        AddMainServiceTypeCase(
            id="recodes_chn_as_care_home_with_nursing",
            input_rows=[(MainServiceID.care_home_services_with_nursing_chn,)],
            expected_rows=[
                (
                    MainServiceID.care_home_services_with_nursing_chn,
                    "Care home with nursing",
                )
            ],
        ),
        AddMainServiceTypeCase(
            id="recodes_chs_as_care_home_without_nursing",
            input_rows=[(MainServiceID.care_home_services_without_nursing_chs,)],
            expected_rows=[
                (
                    MainServiceID.care_home_services_without_nursing_chs,
                    "Care home without nursing",
                )
            ],
        ),
        AddMainServiceTypeCase(
            id="recodes_any_other_service_as_non_residential",
            input_rows=[
                (MainServiceID.adult_placement_home,),
                (MainServiceID.domestic_services_and_home_help,),
            ],
            expected_rows=[
                (MainServiceID.adult_placement_home, "non-residential"),
                (MainServiceID.domestic_services_and_home_help, "non-residential"),
            ],
        ),
    ]

    # Rows: (location_id, worker_id, import_date, employment_status_clean_labelled).
    filter_worker_turnover_data_cases = [
        FilterWorkerTurnoverDataCase(
            id="keeps_permanent_and_temporary_workers",
            input_rows=[
                ("loc_a", "1", START, EmpStat.permanent),
                ("loc_a", "2", START, EmpStat.temporary),
            ],
            expected_rows=[
                ("loc_a", "1", START, EmpStat.permanent),
                ("loc_a", "2", START, EmpStat.temporary),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="removes_bank_or_pool_agency_and_other_workers",
            input_rows=[
                ("loc_a", "1", START, EmpStat.permanent),
                ("loc_a", "2", START, EmpStat.bank_or_pool),
                ("loc_a", "3", START, EmpStat.agency),
                ("loc_a", "4", START, EmpStat.other),
            ],
            expected_rows=[
                ("loc_a", "1", START, EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="removes_workers_with_null_location_id",
            input_rows=[
                ("loc_a", "1", START, EmpStat.permanent),
                (None, "2", START, EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "1", START, EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="removes_rows_before_1_jan_2020_and_keeps_rows_from_then",
            input_rows=[
                ("loc_a", "1", date(2019, 12, 2), EmpStat.permanent),
                ("loc_a", "2", date(2020, 1, 1), EmpStat.permanent),
                ("loc_a", "3", date(2020, 2, 3), EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "2", date(2020, 1, 1), EmpStat.permanent),
                ("loc_a", "3", date(2020, 2, 3), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_only_earliest_import_date_in_each_month",
            input_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
                ("loc_a", "1", date(2021, 1, 12), EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_all_workers_on_earliest_import_date",
            input_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
                ("loc_a", "2", date(2021, 1, 5), EmpStat.temporary),
                ("loc_a", "1", date(2021, 1, 12), EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
                ("loc_a", "2", date(2021, 1, 5), EmpStat.temporary),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_one_import_date_per_month_across_months",
            input_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
                ("loc_a", "1", date(2021, 1, 12), EmpStat.permanent),
                ("loc_a", "1", date(2021, 2, 9), EmpStat.permanent),
                ("loc_a", "1", date(2021, 2, 2), EmpStat.permanent),
                ("loc_a", "1", date(2021, 3, 1), EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
                ("loc_a", "1", date(2021, 2, 2), EmpStat.permanent),
                ("loc_a", "1", date(2021, 3, 1), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_one_import_date_per_month_across_years",
            input_rows=[
                ("loc_a", "1", date(2020, 1, 22), EmpStat.permanent),
                ("loc_a", "1", date(2020, 1, 15), EmpStat.permanent),
                ("loc_a", "1", date(2021, 1, 12), EmpStat.permanent),
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "1", date(2020, 1, 15), EmpStat.permanent),
                ("loc_a", "1", date(2021, 1, 5), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="picks_earliest_import_date_after_removing_other_workers",
            input_rows=[
                ("loc_a", "1", date(2021, 1, 5), EmpStat.agency),
                ("loc_a", "2", date(2021, 1, 12), EmpStat.permanent),
            ],
            expected_rows=[
                ("loc_a", "2", date(2021, 1, 12), EmpStat.permanent),
            ],
        ),
    ]

    # Input rows: (establishment_id, worker_id, import_date).
    # Expected rows: input plus (establishment_in_both_snapshots, is_leaver).
    add_is_leaver_flag_cases = [
        AddIsLeaverFlagCase(
            id="flags_worker_in_end_snapshot_as_not_leaver",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "1", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, False),
                ("est_a", "1", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_worker_missing_from_end_snapshot_as_leaver",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "2", START),
                ("est_a", "1", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, False),
                ("est_a", "2", START, True, True),
                ("est_a", "1", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="matches_end_snapshot_on_month_and_year_ignoring_day",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "1", date(2022, 1, 28)),
            ],
            expected_rows=[
                ("est_a", "1", START, True, False),
                ("est_a", "1", date(2022, 1, 28), None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_worker_as_leaver_when_only_in_other_months",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "2", END),
                ("est_a", "1", date(2022, 2, 5)),
            ],
            expected_rows=[
                ("est_a", "1", START, True, True),
                ("est_a", "2", END, None, None),
                ("est_a", "1", date(2022, 2, 5), None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_same_worker_id_at_other_establishment_as_leaver",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "2", END),
                ("est_b", "1", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, True),
                ("est_a", "2", END, None, None),
                ("est_b", "1", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="nulls_is_leaver_when_establishment_missing_from_end_snapshot",
            input_rows=[
                ("est_a", "1", START),
                ("est_b", "2", END),
            ],
            expected_rows=[
                ("est_a", "1", START, False, None),
                ("est_b", "2", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="nulls_is_leaver_when_establishment_only_in_other_months",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "2", date(2022, 2, 5)),
                ("est_b", "3", END),
            ],
            expected_rows=[
                ("est_a", "1", START, False, None),
                ("est_a", "2", date(2022, 2, 5), None, None),
                ("est_b", "3", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_each_establishment_separately_in_same_month",
            input_rows=[
                ("est_a", "1", START),
                ("est_b", "2", START),
                ("est_b", "2", END),
            ],
            expected_rows=[
                ("est_a", "1", START, False, None),
                ("est_b", "2", START, True, False),
                ("est_b", "2", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="nulls_both_flags_when_no_end_snapshot",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "1", date(2021, 2, 5)),
                ("est_a", "1", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, False),
                ("est_a", "1", date(2021, 2, 5), None, None),
                ("est_a", "1", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="keeps_duplicate_stayers_without_multiplying_rows",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "1", START),
                ("est_a", "1", END),
                ("est_a", "1", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, False),
                ("est_a", "1", START, True, False),
                ("est_a", "1", END, None, None),
                ("est_a", "1", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="keeps_duplicate_leavers",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "1", START),
                ("est_a", "2", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, True),
                ("est_a", "1", START, True, True),
                ("est_a", "2", END, None, None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="keeps_one_row_when_establishment_has_many_end_workers",
            input_rows=[
                ("est_a", "1", START),
                ("est_a", "2", END),
                ("est_a", "3", END),
            ],
            expected_rows=[
                ("est_a", "1", START, True, True),
                ("est_a", "2", END, None, None),
                ("est_a", "3", END, None, None),
            ],
        ),
    ]

    # Input rows: (establishment_id, import_date, save_date).
    # Expected rows: (establishment_id, import_date, latest_save_date).
    get_latest_save_date_per_establishment_cases = [
        GetLatestSaveDatePerEstablishmentCase(
            id="takes_max_save_date_per_establishment_per_import_date",
            input_rows=[
                ("est_a", START, date(2021, 1, 2)),
                ("est_a", START, date(2021, 1, 4)),
                ("est_a", START, date(2020, 12, 1)),
            ],
            expected_rows=[("est_a", START, date(2021, 1, 4))],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="keeps_separate_rows_for_different_import_dates",
            input_rows=[
                ("est_a", START, date(2021, 1, 2)),
                ("est_a", date(2021, 2, 5), date(2021, 1, 20)),
            ],
            expected_rows=[
                ("est_a", START, date(2021, 1, 2)),
                ("est_a", date(2021, 2, 5), date(2021, 1, 20)),
            ],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="keeps_separate_rows_for_different_establishments",
            input_rows=[
                ("est_a", START, date(2021, 1, 2)),
                ("est_b", START, date(2020, 6, 1)),
            ],
            expected_rows=[
                ("est_a", START, date(2021, 1, 2)),
                ("est_b", START, date(2020, 6, 1)),
            ],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="ignores_null_save_dates_when_another_exists",
            input_rows=[
                ("est_a", START, None),
                ("est_a", START, date(2021, 1, 3)),
            ],
            expected_rows=[("est_a", START, date(2021, 1, 3))],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="returns_null_when_all_save_dates_null",
            input_rows=[("est_a", START, None), ("est_a", START, None)],
            expected_rows=[("est_a", START, None)],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="keeps_only_import_dates_present_in_snapshot_rows",
            input_rows=[
                ("est_a", END, date(2021, 4, 1)),
                ("est_a", date(2022, 1, 20), date(2022, 1, 18)),
            ],
            snapshot_rows=[("est_a", END, None)],
            expected_rows=[("est_a", END, date(2021, 4, 1))],
        ),
    ]

    # Worker rows: (establishment_id, worker_id, import_date, is_leaver).
    # Save date rows: (establishment_id, import_date, latest_save_date).
    # Expected rows: worker row plus (months_since_save_start,
    # months_since_save_end, establishment_active).
    add_establishment_active_flag_cases = [
        AddEstablishmentActiveFlagCase(
            id="flags_active_when_both_snapshots_within_n_months",
            worker_rows=[("est_a", "1", START, False), ("est_a", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 9, 20)),
                ("est_a", END, date(2021, 9, 10)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 4, 4, True),
                ("est_a", "1", END, None, 4, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_inactive_when_start_snapshot_older_than_n_months",
            worker_rows=[("est_a", "1", START, False), ("est_a", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 5, 10)),
                ("est_a", END, date(2021, 11, 2)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 8, 2, False),
                ("est_a", "1", END, None, 2, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_inactive_when_end_snapshot_older_than_n_months",
            worker_rows=[("est_a", "1", START, False), ("est_a", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 12, 1)),
                ("est_a", END, date(2021, 3, 10)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 1, 10, False),
                ("est_a", "1", END, None, 10, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_active_when_gap_equals_n_months",
            worker_rows=[("est_a", "1", START, False), ("est_a", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 7, 31)),
                ("est_a", END, date(2021, 7, 1)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 6, 6, True),
                ("est_a", "1", END, None, 6, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="respects_n_months_parameter",
            n_months=3,
            worker_rows=[("est_a", "1", START, False), ("est_a", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 9, 20)),
                ("est_a", END, date(2021, 9, 10)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 4, 4, False),
                ("est_a", "1", END, None, 4, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="calculates_whole_calendar_month_gap_for_both_snapshots",
            worker_rows=[("est_a", "1", START, False), ("est_a", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 12, 31)),  # 5 days, 1 month
                ("est_a", END, date(2020, 1, 1)),  # 24 months
            ],
            expected_rows=[
                ("est_a", "1", START, False, 1, 24, False),
                ("est_a", "1", END, None, 24, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_inactive_with_null_gap_when_no_save_date",
            worker_rows=[
                ("est_a", "1", START, False),
                ("est_a", "1", END, None),
                ("est_b", "1", START, True),
            ],
            save_date_rows=[
                ("est_a", START, None),
                ("est_a", END, date(2021, 12, 1)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, None, 1, False),
                ("est_a", "1", END, None, 1, None, None),
                ("est_b", "1", START, True, None, None, False),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="leaves_flag_and_end_gap_null_when_no_end_snapshot",
            worker_rows=[("est_a", "1", START, None)],
            save_date_rows=[("est_a", START, date(2020, 12, 1))],
            expected_rows=[("est_a", "1", START, None, 1, None, None)],
        ),
        AddEstablishmentActiveFlagCase(
            id="keeps_negative_gap_when_save_date_after_import_date",
            worker_rows=[
                ("est_a", "1", date(2021, 9, 30), False),
                ("est_a", "1", date(2022, 9, 30), None),
            ],
            save_date_rows=[
                ("est_a", date(2021, 9, 30), date(2021, 10, 2)),
                ("est_a", date(2022, 9, 30), date(2022, 9, 29)),
            ],
            expected_rows=[
                ("est_a", "1", date(2021, 9, 30), False, -1, 0, True),
                ("est_a", "1", date(2022, 9, 30), None, 0, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="keeps_all_input_rows_and_is_leaver_unchanged",
            worker_rows=[
                ("est_a", "1", START, False),
                ("est_a", "1", START, False),
                ("est_a", "2", START, True),
                ("est_a", "1", END, None),
            ],
            save_date_rows=[
                ("est_a", START, date(2020, 12, 1)),
                ("est_a", END, date(2021, 12, 1)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 1, 1, True),
                ("est_a", "1", START, False, 1, 1, True),
                ("est_a", "2", START, True, 1, 1, True),
                ("est_a", "1", END, None, 1, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="matches_end_gap_only_to_snapshot_12_months_later",
            worker_rows=[
                ("est_a", "1", START, False),
                ("est_a", "1", date(2021, 12, 5), None),
                ("est_a", "1", END, None),
                ("est_a", "1", date(2022, 2, 5), None),
            ],
            save_date_rows=[
                ("est_a", START, date(2020, 12, 1)),
                ("est_a", date(2021, 12, 5), date(2021, 1, 1)),  # 11 months later
                ("est_a", END, date(2021, 12, 1)),
                ("est_a", date(2022, 2, 5), date(2021, 1, 1)),  # 13 months later
            ],
            expected_rows=[
                ("est_a", "1", START, False, 1, 1, True),
                ("est_a", "1", date(2021, 12, 5), None, 11, None, None),
                ("est_a", "1", END, None, 1, None, None),
                ("est_a", "1", date(2022, 2, 5), None, 13, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_each_establishment_from_its_own_save_dates",
            worker_rows=[
                ("est_a", "1", START, False),
                ("est_a", "1", END, None),
                ("est_b", "1", START, False),
                ("est_b", "1", END, None),
            ],
            save_date_rows=[
                ("est_a", START, date(2020, 12, 1)),
                ("est_a", END, date(2021, 12, 1)),
                ("est_b", START, date(2020, 12, 1)),
                ("est_b", END, date(2020, 1, 1)),
            ],
            expected_rows=[
                ("est_a", "1", START, False, 1, 1, True),
                ("est_a", "1", END, None, 1, None, None),
                ("est_b", "1", START, False, 1, 24, False),
                ("est_b", "1", END, None, 24, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="keeps_gaps_and_nulls_flag_when_is_leaver_null_with_save_dates",
            worker_rows=[("est_a", "1", START, None), ("est_b", "1", END, None)],
            save_date_rows=[
                ("est_a", START, date(2020, 12, 1)),
                ("est_a", END, date(2021, 12, 1)),
                ("est_b", END, date(2021, 12, 1)),
            ],
            expected_rows=[
                ("est_a", "1", START, None, 1, 1, None),
                ("est_b", "1", END, None, 1, None, None),
            ],
        ),
    ]
