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
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.ascwds_labelled_vocab import MainServiceID

START = date(2021, 1, 5)
END = date(2022, 1, 5)  # 12 months after START
MID = date(2021, 6, 1)  # between START and END
BEFORE_START = date(2020, 6, 1)


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
class CombinePayAndHoursCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


@dataclass
class ImputeWorkerColumnsCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


@dataclass
class AddNlwAndPayDifferenceCase:
    id: str
    expected_rows: list[tuple[Any, ...]]


@dataclass
class AddCodeGroupCase:
    id: str
    code_column: str
    group_column: str
    expected_rows: list[tuple[Any, ...]]


# The EEA codes, written out here rather than imported so the test checks the
# constant in the job.
EEA_CODES = [
    "40", "56", "100", "196", "203", "208", "233", "246", "250", "276",
    "300", "348", "372", "380", "428", "440", "442", "470", "528", "616",
    "620", "642", "703", "705", "724", "752", "352", "438", "578", "191",
]  # fmt: skip


def build_code_group_cases(
    code_column: str, group_column: str, eea_label: str, uk_label: str
) -> list[AddCodeGroupCase]:
    """Builds the code to group cases shared by nationality and country of birth."""
    return [
        AddCodeGroupCase(
            id=f"{code_column}_eea_when_code_is_eea",
            code_column=code_column,
            group_column=group_column,
            expected_rows=[("250", eea_label)],
        ),
        AddCodeGroupCase(
            id=f"{code_column}_uk_when_code_is_826",
            code_column=code_column,
            group_column=group_column,
            expected_rows=[("826", uk_label)],
        ),
        AddCodeGroupCase(
            id=f"{code_column}_non_eea_when_code_is_other",
            code_column=code_column,
            group_column=group_column,
            expected_rows=[("356", "Non-EEA")],
        ),
        AddCodeGroupCase(
            id=f"{code_column}_null_when_code_is_null",
            code_column=code_column,
            group_column=group_column,
            expected_rows=[(None, None)],
        ),
        AddCodeGroupCase(
            id=f"{code_column}_null_when_code_is_blank",
            code_column=code_column,
            group_column=group_column,
            expected_rows=[("", None)],
        ),
    ]


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
class AddTurnoverRateCase:
    id: str
    expected_rows: list[tuple[Any, ...]]


@dataclass
class AddHeadcountGapCase:
    id: str
    expected_rows: list[tuple[Any, ...]]


@dataclass
class AddValidForAnalysisFlagCase:
    id: str
    expected_rows: list[tuple[Any, ...]]


@dataclass
class WorkerTurnoverData:
    # Rows: (establishment_id, worker_id, import_date,
    # establishment_in_both_snapshots, is_leaver, turnover_rate).
    add_turnover_rate_cases = [
        AddTurnoverRateCase(
            id="returns_leavers_divided_by_start_employees",
            expected_rows=[
                ("est_a", "1", START, True, True, 0.25),
                ("est_a", "2", START, True, False, 0.25),
                ("est_a", "3", START, True, False, 0.25),
                ("est_a", "4", START, True, False, 0.25),
            ],
        ),
        AddTurnoverRateCase(
            id="returns_zero_when_no_leavers",
            expected_rows=[("est_a", "1", START, True, False, 0.0)],
        ),
        AddTurnoverRateCase(
            id="calculates_rate_separately_per_establishment",
            expected_rows=[
                ("est_a", "1", START, True, True, 0.5),
                ("est_a", "2", START, True, False, 0.5),
                ("est_b", "1", START, True, False, 0.0),
            ],
        ),
        AddTurnoverRateCase(
            id="calculates_rate_separately_per_start_month",
            expected_rows=[
                ("est_a", "1", START, True, True, 1.0),
                ("est_a", "1", date(2021, 2, 5), True, False, 0.0),
            ],
        ),
        AddTurnoverRateCase(
            id="returns_null_when_establishment_not_in_both_snapshots",
            expected_rows=[("est_a", "1", START, False, None, None)],
        ),
        AddTurnoverRateCase(
            id="returns_null_when_no_end_snapshot",
            expected_rows=[("est_a", "1", START, None, None, None)],
        ),
    ]

    # Rows: (establishment_id, worker_id, import_date, created_date, is_leaver,
    # headcount_reconciliation_gap, headcount_gap_within_tolerance). Tolerance is 1.
    add_headcount_reconciliation_gap_cases = [
        AddHeadcountGapCase(
            id="returns_zero_when_headcounts_reconcile",
            expected_rows=[
                ("est_a", "1", START, None, False, 0, True),
                ("est_a", "2", START, None, False, 0, True),
                ("est_a", "3", START, None, True, 0, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_a", "2", END, None, None, None, None),
                ("est_a", "4", END, MID, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="returns_positive_gap_when_workers_missing_from_end",
            expected_rows=[
                ("est_a", "1", START, None, False, 1, True),
                ("est_a", "2", START, None, False, 1, True),
                ("est_a", "1", END, None, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="returns_negative_gap_when_unexplained_workers_at_end",
            expected_rows=[
                ("est_a", "1", START, None, False, -1, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_a", "2", END, BEFORE_START, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="flags_false_when_positive_gap_above_tolerance",
            expected_rows=[
                ("est_a", "1", START, None, False, 2, False),
                ("est_a", "2", START, None, False, 2, False),
                ("est_a", "3", START, None, False, 2, False),
                ("est_a", "1", END, None, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="flags_false_when_negative_gap_beyond_tolerance",
            expected_rows=[
                ("est_a", "1", START, None, False, -2, False),
                ("est_a", "1", END, None, None, None, None),
                ("est_a", "2", END, BEFORE_START, None, None, None),
                ("est_a", "3", END, BEFORE_START, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="does_not_count_worker_created_on_start_date",
            expected_rows=[
                ("est_a", "1", START, None, False, -1, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_a", "2", END, START, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="counts_worker_created_on_end_date",
            expected_rows=[
                ("est_a", "1", START, None, False, 0, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_a", "2", END, END, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="does_not_count_worker_with_null_created_date",
            expected_rows=[
                ("est_a", "1", START, None, False, -1, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_a", "2", END, None, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="returns_null_when_establishment_not_in_end_snapshot",
            expected_rows=[
                ("est_a", "1", START, None, False, 0, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_b", "1", START, None, None, None, None),
            ],
        ),
        AddHeadcountGapCase(
            id="returns_null_when_no_end_snapshot",
            expected_rows=[("est_a", "1", START, None, None, None, None)],
        ),
        AddHeadcountGapCase(
            id="calculates_gap_separately_per_establishment",
            expected_rows=[
                ("est_a", "1", START, None, False, 0, True),
                ("est_a", "1", END, None, None, None, None),
                ("est_b", "1", START, None, False, 2, False),
                ("est_b", "2", START, None, False, 2, False),
                ("est_b", "3", START, None, False, 2, False),
                ("est_b", "1", END, None, None, None, None),
            ],
        ),
    ]

    # Rows: (establishment_id, worker_id, import_date,
    # establishment_in_both_snapshots, establishment_active,
    # headcount_gap_within_tolerance, turnover_rate, plausible_turnover,
    # valid_for_analysis).
    add_valid_for_analysis_flag_cases = [
        AddValidForAnalysisFlagCase(
            id="valid_when_all_components_true",
            expected_rows=[("est_a", "1", START, True, True, True, 0.25, True, True)],
        ),
        AddValidForAnalysisFlagCase(
            id="implausible_and_invalid_when_rate_is_zero",
            expected_rows=[("est_a", "1", START, True, True, True, 0.0, False, False)],
        ),
        AddValidForAnalysisFlagCase(
            id="implausible_and_invalid_when_rate_is_one",
            expected_rows=[("est_a", "1", START, True, True, True, 1.0, False, False)],
        ),
        AddValidForAnalysisFlagCase(
            id="plausible_when_rate_just_above_zero",
            expected_rows=[("est_a", "1", START, True, True, True, 0.125, True, True)],
        ),
        AddValidForAnalysisFlagCase(
            id="plausible_when_rate_just_below_one",
            expected_rows=[("est_a", "1", START, True, True, True, 0.875, True, True)],
        ),
        AddValidForAnalysisFlagCase(
            id="invalid_when_establishment_inactive",
            expected_rows=[("est_a", "1", START, True, False, True, 0.25, True, False)],
        ),
        AddValidForAnalysisFlagCase(
            id="invalid_when_headcount_gap_outside_tolerance",
            expected_rows=[("est_a", "1", START, True, True, False, 0.25, True, False)],
        ),
        AddValidForAnalysisFlagCase(
            id="invalid_when_establishment_not_in_both_snapshots",
            expected_rows=[("est_a", "1", START, False, None, None, None, None, False)],
        ),
        AddValidForAnalysisFlagCase(
            id="invalid_when_no_end_snapshot",
            expected_rows=[("est_a", "1", START, None, None, None, None, None, False)],
        ),
    ]

    add_country_code_group_cases = [
        *build_code_group_cases(
            code_column=AWKClean.nationality,
            group_column=HackCols.nationality_group,
            eea_label="EEA (non-British)",
            uk_label="British",
        ),
        *build_code_group_cases(
            code_column=AWKClean.country_of_birth,
            group_column=HackCols.country_of_birth_group,
            eea_label="EEA (non-UK)",
            uk_label="UK",
        ),
    ]

    # Rows: (worker_id, hourly_rate, salary, contracted_hours, average_hours);
    # expected rows are (worker_id, hours, hourly_pay). 20800 / (40 * 52) is
    # exactly 10.
    combine_pay_and_hours_cases = [
        CombinePayAndHoursCase(
            id="rate_without_hours",
            input_rows=[("1", 12.5, None, None, None)],
            expected_rows=[("1", None, 12.5)],
        ),
        CombinePayAndHoursCase(
            id="rate_and_salary_uses_rate",
            input_rows=[("1", 12.5, 20800.0, 40.0, None)],
            expected_rows=[("1", 40.0, 12.5)],
        ),
        CombinePayAndHoursCase(
            id="rate_with_contracted_hours",
            input_rows=[("1", 12.5, None, 40.0, None)],
            expected_rows=[("1", 40.0, 12.5)],
        ),
        CombinePayAndHoursCase(
            id="salary_with_contracted_hours",
            input_rows=[("1", None, 20800.0, 40.0, None)],
            expected_rows=[("1", 40.0, 10.0)],
        ),
        CombinePayAndHoursCase(
            id="salary_with_average_hours",
            input_rows=[("1", None, 10400.0, None, 20.0)],
            expected_rows=[("1", 20.0, 10.0)],
        ),
        CombinePayAndHoursCase(
            id="salary_without_hours",
            input_rows=[("1", None, 20800.0, None, None)],
            expected_rows=[("1", None, None)],
        ),
        CombinePayAndHoursCase(
            id="salary_with_zero_contracted_hours_uses_average_hours",
            input_rows=[("1", None, 11960.0, 0.0, 23.0)],
            expected_rows=[("1", 23.0, 10.0)],
        ),
        CombinePayAndHoursCase(
            id="salary_with_zero_hours_in_both",
            input_rows=[("1", None, 20800.0, 0.0, 0.0)],
            expected_rows=[("1", None, None)],
        ),
        CombinePayAndHoursCase(
            id="nothing_given",
            input_rows=[("1", None, None, None, None)],
            expected_rows=[("1", None, None)],
        ),
    ]

    # Rows: (establishment_id, worker_id, import_date, hours).
    impute_worker_columns_cases = [
        ImputeWorkerColumnsCase(
            id="fills_null_from_earlier_value",
            input_rows=[("e1", "w1", START, 10.0), ("e1", "w1", MID, None)],
            expected_rows=[("e1", "w1", START, 10.0), ("e1", "w1", MID, 10.0)],
        ),
        ImputeWorkerColumnsCase(
            id="fills_leading_null_from_later_value",
            input_rows=[("e1", "w1", START, None), ("e1", "w1", MID, 20.0)],
            expected_rows=[("e1", "w1", START, 20.0), ("e1", "w1", MID, 20.0)],
        ),
        ImputeWorkerColumnsCase(
            id="prefers_earlier_value_over_later_value",
            input_rows=[
                ("e1", "w1", START, 10.0),
                ("e1", "w1", MID, None),
                ("e1", "w1", END, 30.0),
            ],
            expected_rows=[
                ("e1", "w1", START, 10.0),
                ("e1", "w1", MID, 10.0),
                ("e1", "w1", END, 30.0),
            ],
        ),
        ImputeWorkerColumnsCase(
            id="orders_by_import_date_not_row_order",
            input_rows=[("e1", "w1", MID, None), ("e1", "w1", START, 10.0)],
            expected_rows=[("e1", "w1", MID, 10.0), ("e1", "w1", START, 10.0)],
        ),
        ImputeWorkerColumnsCase(
            id="keeps_null_when_never_known",
            input_rows=[("e1", "w1", START, None), ("e1", "w1", MID, None)],
            expected_rows=[("e1", "w1", START, None), ("e1", "w1", MID, None)],
        ),
        ImputeWorkerColumnsCase(
            id="does_not_fill_across_workers",
            input_rows=[("e1", "w1", START, 10.0), ("e1", "w2", START, None)],
            expected_rows=[("e1", "w1", START, 10.0), ("e1", "w2", START, None)],
        ),
        ImputeWorkerColumnsCase(
            id="does_not_fill_across_establishments",
            input_rows=[("e1", "w1", START, 10.0), ("e2", "w1", MID, None)],
            expected_rows=[("e1", "w1", START, 10.0), ("e2", "w1", MID, None)],
        ),
    ]

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

    # Rows: (import_date, apprentice_labels, age, hourly_pay, nlw_hourly_rate,
    # hourly_pay_minus_nlw); the input is these rows without the last two columns.
    add_nlw_and_pay_difference_cases = [
        AddNlwAndPayDifferenceCase(
            id="import_on_1_april_uses_previous_rate",
            expected_rows=[(date(2021, 4, 1), "No", 30.0, 10.0, 8.72, 1.28)],
        ),
        AddNlwAndPayDifferenceCase(
            id="import_after_1_april_uses_new_rate",
            expected_rows=[(date(2021, 4, 2), "No", 30.0, 10.0, 8.91, 1.09)],
        ),
        AddNlwAndPayDifferenceCase(
            id="earliest_import_uses_april_2019_rate",
            expected_rows=[(date(2020, 1, 1), "No", 30.0, 10.0, 8.21, 1.79)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_17_uses_under_18_rate_2019",
            expected_rows=[(date(2020, 1, 1), "No", 17.0, 5.0, 4.35, 0.65)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_18_uses_18_to_20_rate_2019",
            expected_rows=[(date(2020, 1, 1), "No", 18.0, 7.0, 6.15, 0.85)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_24_uses_21_to_24_rate_2019",
            expected_rows=[(date(2020, 1, 1), "No", 24.0, 8.0, 7.70, 0.30)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_25_uses_oldest_rate_2019",
            expected_rows=[(date(2020, 1, 1), "No", 25.0, 9.0, 8.21, 0.79)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_22_uses_21_to_22_rate_2021",
            expected_rows=[(date(2021, 6, 1), "No", 22.0, 9.0, 8.36, 0.64)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_23_uses_oldest_rate_2021",
            expected_rows=[(date(2021, 6, 1), "No", 23.0, 9.0, 8.91, 0.09)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_21_uses_oldest_rate_2024",
            expected_rows=[(date(2024, 6, 1), "No", 21.0, 12.0, 11.44, 0.56)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_20_uses_18_to_20_rate_2024",
            expected_rows=[(date(2024, 6, 1), "No", 20.0, 9.0, 8.60, 0.40)],
        ),
        AddNlwAndPayDifferenceCase(
            id="latest_rates_from_april_2026",
            expected_rows=[(date(2026, 4, 2), "No", 30.0, 13.0, 12.71, 0.29)],
        ),
        AddNlwAndPayDifferenceCase(
            id="apprentice_uses_apprentice_rate_whatever_age",
            expected_rows=[(date(2024, 6, 1), "Yes", 40.0, 7.0, 6.40, 0.60)],
        ),
        AddNlwAndPayDifferenceCase(
            id="apprentice_with_null_age_uses_apprentice_rate",
            expected_rows=[(date(2024, 6, 1), "Yes", None, 7.0, 6.40, 0.60)],
        ),
        AddNlwAndPayDifferenceCase(
            id="apprentice_not_known_uses_age_band",
            expected_rows=[(date(2024, 6, 1), "Not known", 19.0, 9.0, 8.60, 0.40)],
        ),
        AddNlwAndPayDifferenceCase(
            id="null_apprentice_uses_age_band",
            expected_rows=[(date(2024, 6, 1), None, 19.0, 9.0, 8.60, 0.40)],
        ),
        AddNlwAndPayDifferenceCase(
            id="null_age_not_apprentice_uses_oldest_rate",
            expected_rows=[(date(2025, 6, 1), "No", None, 13.0, 12.21, 0.79)],
        ),
        AddNlwAndPayDifferenceCase(
            id="age_over_lookup_maximum_uses_oldest_rate",
            expected_rows=[(date(2025, 6, 1), "No", 150.0, 13.0, 12.21, 0.79)],
        ),
        AddNlwAndPayDifferenceCase(
            id="null_hourly_pay_gives_null_difference",
            expected_rows=[(date(2025, 6, 1), "No", 30.0, None, 12.21, None)],
        ),
    ]
