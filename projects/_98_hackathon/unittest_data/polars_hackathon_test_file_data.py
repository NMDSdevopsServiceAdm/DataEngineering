from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat

START = date(2021, 1, 5)
END = date(2022, 1, 5)  # 12 months after START


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
class AddMainServiceHackathonCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


@dataclass
class WorkerTurnoverData:
    add_main_service_hackathon_cases = [
        AddMainServiceHackathonCase(
            id="recodes_code_1_as_care_home_with_nursing",
            input_rows=[("1",)],
            expected_rows=[("1", "Care home with nursing")],
        ),
        AddMainServiceHackathonCase(
            id="recodes_code_2_as_care_home_without_nursing",
            input_rows=[("2",)],
            expected_rows=[("2", "Care home without nursing")],
        ),
        AddMainServiceHackathonCase(
            id="recodes_any_other_code_as_non_residential",
            input_rows=[("3",), ("10",)],
            expected_rows=[("3", "non-residential"), ("10", "non-residential")],
        ),
        AddMainServiceHackathonCase(
            id="keeps_null_main_service_as_null",
            input_rows=[(None,)],
            expected_rows=[(None, None)],
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
