from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat


def worker_row(
    worker_id: str,
    import_date: date,
    employment_status_label: str = EmpStat.permanent,
    establishment_id: str = "1-001",
    save_date: date | None = None,
) -> tuple[Any, ...]:
    """Returns a cleaned worker row in schema column order."""
    return (
        "1-001",
        establishment_id,
        worker_id,
        "1",
        "1",
        import_date,
        save_date,
        "1",
        "care_worker",
        "1",
        employment_status_label,
    )


def flagged_row(
    worker_id: str,
    import_date: date,
    is_leaver: bool | None,
    establishment_id: str = "1-001",
) -> tuple[Any, ...]:
    """Returns a permanent worker row with `is_leaver` appended."""
    return (
        *worker_row(worker_id, import_date, establishment_id=establishment_id),
        is_leaver,
    )


def active_row(
    worker_id: str,
    import_date: date,
    is_leaver: bool | None,
    months_since_save_start: int | None,
    months_since_save_end: int | None,
    establishment_active: bool | None,
    establishment_id: str = "1-001",
) -> tuple[Any, ...]:
    """Returns a flagged worker row with the three establishment activity columns."""
    return (
        *flagged_row(worker_id, import_date, is_leaver, establishment_id),
        months_since_save_start,
        months_since_save_end,
        establishment_active,
    )


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
    filter_worker_turnover_data_cases = [
        FilterWorkerTurnoverDataCase(
            id="keeps_permanent_and_temporary_workers",
            input_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("2", date(2021, 1, 5), EmpStat.temporary),
            ],
            expected_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("2", date(2021, 1, 5), EmpStat.temporary),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="removes_bank_or_pool_agency_and_other_workers",
            input_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("2", date(2021, 1, 5), EmpStat.bank_or_pool),
                worker_row("3", date(2021, 1, 5), EmpStat.agency),
                worker_row("4", date(2021, 1, 5), EmpStat.other),
            ],
            expected_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="removes_rows_before_1_jan_2020_and_keeps_rows_from_then",
            input_rows=[
                worker_row("1", date(2019, 12, 2), EmpStat.permanent),
                worker_row("2", date(2020, 1, 1), EmpStat.permanent),
                worker_row("3", date(2020, 2, 3), EmpStat.permanent),
            ],
            expected_rows=[
                worker_row("2", date(2020, 1, 1), EmpStat.permanent),
                worker_row("3", date(2020, 2, 3), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_only_earliest_import_date_in_each_month",
            input_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("1", date(2021, 1, 12), EmpStat.permanent),
            ],
            expected_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_all_workers_on_earliest_import_date",
            input_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("2", date(2021, 1, 5), EmpStat.temporary),
                worker_row("1", date(2021, 1, 12), EmpStat.permanent),
            ],
            expected_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("2", date(2021, 1, 5), EmpStat.temporary),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_one_import_date_per_month_across_months",
            input_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("1", date(2021, 1, 12), EmpStat.permanent),
                worker_row("1", date(2021, 2, 9), EmpStat.permanent),
                worker_row("1", date(2021, 2, 2), EmpStat.permanent),
                worker_row("1", date(2021, 3, 1), EmpStat.permanent),
            ],
            expected_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
                worker_row("1", date(2021, 2, 2), EmpStat.permanent),
                worker_row("1", date(2021, 3, 1), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="keeps_one_import_date_per_month_across_years",
            input_rows=[
                worker_row("1", date(2020, 1, 22), EmpStat.permanent),
                worker_row("1", date(2020, 1, 15), EmpStat.permanent),
                worker_row("1", date(2021, 1, 12), EmpStat.permanent),
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
            ],
            expected_rows=[
                worker_row("1", date(2020, 1, 15), EmpStat.permanent),
                worker_row("1", date(2021, 1, 5), EmpStat.permanent),
            ],
        ),
        FilterWorkerTurnoverDataCase(
            id="picks_earliest_import_date_after_removing_other_workers",
            input_rows=[
                worker_row("1", date(2021, 1, 5), EmpStat.agency),
                worker_row("2", date(2021, 1, 12), EmpStat.permanent),
            ],
            expected_rows=[
                worker_row("2", date(2021, 1, 12), EmpStat.permanent),
            ],
        ),
    ]

    add_is_leaver_flag_cases = [
        AddIsLeaverFlagCase(
            id="flags_worker_in_end_snapshot_as_not_leaver",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2022, 1, 5)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_worker_missing_from_end_snapshot_as_leaver",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("2", date(2021, 1, 5)),
                worker_row("1", date(2022, 1, 5)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("2", date(2021, 1, 5), True),
                flagged_row("1", date(2022, 1, 5), None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="matches_end_snapshot_on_month_and_year_ignoring_day",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2022, 1, 28)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 28), None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_worker_as_leaver_when_only_in_other_months",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("2", date(2022, 1, 5)),
                worker_row("1", date(2022, 2, 5)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), True),
                flagged_row("2", date(2022, 1, 5), None),
                flagged_row("1", date(2022, 2, 5), None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="flags_same_worker_id_at_other_establishment_as_leaver",
            input_rows=[
                worker_row("1", date(2021, 1, 5), establishment_id="1-001"),
                worker_row("1", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), True, establishment_id="1-001"),
                flagged_row("1", date(2022, 1, 5), None, establishment_id="1-002"),
            ],
        ),
        AddIsLeaverFlagCase(
            id="leaves_is_leaver_null_when_no_end_snapshot",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2021, 2, 5)),
                worker_row("1", date(2022, 1, 5)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2021, 2, 5), None),
                flagged_row("1", date(2022, 1, 5), None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="keeps_duplicate_stayers_without_multiplying_rows",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2022, 1, 5)),
                worker_row("1", date(2022, 1, 5)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
                flagged_row("1", date(2022, 1, 5), None),
            ],
        ),
        AddIsLeaverFlagCase(
            id="keeps_duplicate_leavers",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2021, 1, 5)),
                worker_row("2", date(2022, 1, 5)),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), True),
                flagged_row("1", date(2021, 1, 5), True),
                flagged_row("2", date(2022, 1, 5), None),
            ],
        ),
    ]

    get_latest_save_date_per_establishment_cases = [
        GetLatestSaveDatePerEstablishmentCase(
            id="takes_max_save_date_per_establishment_per_import_date",
            input_rows=[
                worker_row("1", date(2021, 1, 5), save_date=date(2021, 1, 2)),
                worker_row("2", date(2021, 1, 5), save_date=date(2021, 1, 4)),
                worker_row("3", date(2021, 1, 5), save_date=date(2020, 12, 1)),
            ],
            expected_rows=[("1-001", date(2021, 1, 5), date(2021, 1, 4))],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="keeps_separate_rows_for_different_import_dates",
            input_rows=[
                worker_row("1", date(2021, 1, 5), save_date=date(2021, 1, 2)),
                worker_row("1", date(2021, 2, 5), save_date=date(2021, 1, 20)),
            ],
            expected_rows=[
                ("1-001", date(2021, 1, 5), date(2021, 1, 2)),
                ("1-001", date(2021, 2, 5), date(2021, 1, 20)),
            ],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="keeps_separate_rows_for_different_establishments",
            input_rows=[
                worker_row(
                    "1",
                    date(2021, 1, 5),
                    establishment_id="1-001",
                    save_date=date(2021, 1, 2),
                ),
                worker_row(
                    "2",
                    date(2021, 1, 5),
                    establishment_id="1-002",
                    save_date=date(2020, 6, 1),
                ),
            ],
            expected_rows=[
                ("1-001", date(2021, 1, 5), date(2021, 1, 2)),
                ("1-002", date(2021, 1, 5), date(2020, 6, 1)),
            ],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="ignores_null_save_dates_when_another_exists",
            input_rows=[
                worker_row("1", date(2021, 1, 5), save_date=None),
                worker_row("2", date(2021, 1, 5), save_date=date(2021, 1, 3)),
            ],
            expected_rows=[("1-001", date(2021, 1, 5), date(2021, 1, 3))],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="returns_null_when_all_save_dates_null",
            input_rows=[
                worker_row("1", date(2021, 1, 5), save_date=None),
                worker_row("2", date(2021, 1, 5), save_date=None),
            ],
            expected_rows=[("1-001", date(2021, 1, 5), None)],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="includes_workers_of_every_employment_status",
            input_rows=[
                worker_row(
                    "1", date(2021, 1, 5), EmpStat.permanent, save_date=date(2021, 1, 1)
                ),
                worker_row(
                    "2", date(2021, 1, 5), EmpStat.agency, save_date=date(2021, 1, 4)
                ),
            ],
            expected_rows=[("1-001", date(2021, 1, 5), date(2021, 1, 4))],
        ),
        GetLatestSaveDatePerEstablishmentCase(
            id="keeps_only_import_dates_present_in_snapshot_rows",
            input_rows=[
                worker_row("1", date(2022, 1, 5), save_date=date(2021, 4, 1)),
                worker_row("1", date(2022, 1, 20), save_date=date(2022, 1, 18)),
            ],
            snapshot_rows=[worker_row("1", date(2022, 1, 5))],
            expected_rows=[("1-001", date(2022, 1, 5), date(2021, 4, 1))],
        ),
    ]

    add_establishment_active_flag_cases = [
        AddEstablishmentActiveFlagCase(
            id="flags_active_when_both_snapshots_within_n_months",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 9, 20)),
                ("1-001", date(2022, 1, 5), date(2021, 9, 10)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 4, 4, True),
                active_row("1", date(2022, 1, 5), None, 4, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_inactive_when_start_snapshot_older_than_n_months",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 5, 10)),
                ("1-001", date(2022, 1, 5), date(2021, 11, 2)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 8, 2, False),
                active_row("1", date(2022, 1, 5), None, 2, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_inactive_when_end_snapshot_older_than_n_months",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 12, 1)),
                ("1-001", date(2022, 1, 5), date(2021, 3, 10)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 1, 10, False),
                active_row("1", date(2022, 1, 5), None, 10, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_active_when_gap_equals_n_months",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 7, 31)),
                ("1-001", date(2022, 1, 5), date(2021, 7, 1)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 6, 6, True),
                active_row("1", date(2022, 1, 5), None, 6, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="respects_n_months_parameter",
            n_months=3,
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 9, 20)),
                ("1-001", date(2022, 1, 5), date(2021, 9, 10)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 4, 4, False),
                active_row("1", date(2022, 1, 5), None, 4, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="calculates_whole_calendar_month_gap_for_both_snapshots",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 12, 31)),  # 5 days, 1 month
                ("1-001", date(2022, 1, 5), date(2020, 1, 1)),  # 24 months
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 1, 24, False),
                active_row("1", date(2022, 1, 5), None, 24, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="flags_inactive_with_null_gap_when_no_save_date",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2022, 1, 5), None),
                flagged_row("2", date(2021, 1, 5), True, establishment_id="1-002"),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), None),
                ("1-001", date(2022, 1, 5), date(2021, 12, 1)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, None, 1, False),
                active_row("1", date(2022, 1, 5), None, 1, None, None),
                active_row(
                    "2",
                    date(2021, 1, 5),
                    True,
                    None,
                    None,
                    False,
                    establishment_id="1-002",
                ),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="leaves_flag_and_end_gap_null_when_no_end_snapshot",
            worker_rows=[flagged_row("1", date(2021, 1, 5), None)],
            save_date_rows=[("1-001", date(2021, 1, 5), date(2020, 12, 1))],
            expected_rows=[active_row("1", date(2021, 1, 5), None, 1, None, None)],
        ),
        AddEstablishmentActiveFlagCase(
            id="keeps_negative_gap_when_save_date_after_import_date",
            worker_rows=[
                flagged_row("1", date(2021, 9, 30), False),
                flagged_row("1", date(2022, 9, 30), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 9, 30), date(2021, 10, 2)),
                ("1-001", date(2022, 9, 30), date(2022, 9, 29)),
            ],
            expected_rows=[
                active_row("1", date(2021, 9, 30), False, -1, 0, True),
                active_row("1", date(2022, 9, 30), None, 0, None, None),
            ],
        ),
        AddEstablishmentActiveFlagCase(
            id="keeps_all_input_rows_and_is_leaver_unchanged",
            worker_rows=[
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("1", date(2021, 1, 5), False),
                flagged_row("2", date(2021, 1, 5), True),
                flagged_row("1", date(2022, 1, 5), None),
            ],
            save_date_rows=[
                ("1-001", date(2021, 1, 5), date(2020, 12, 1)),
                ("1-001", date(2022, 1, 5), date(2021, 12, 1)),
            ],
            expected_rows=[
                active_row("1", date(2021, 1, 5), False, 1, 1, True),
                active_row("1", date(2021, 1, 5), False, 1, 1, True),
                active_row("2", date(2021, 1, 5), True, 1, 1, True),
                active_row("1", date(2022, 1, 5), None, 1, None, None),
            ],
        ),
    ]
