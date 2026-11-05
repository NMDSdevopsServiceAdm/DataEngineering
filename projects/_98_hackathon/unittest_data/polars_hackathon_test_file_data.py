from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat


def worker_row(
    worker_id: str,
    import_date: date,
    employment_status_label: str = EmpStat.permanent,
    establishment_id: str = "1-001",
    location_id: str | None = "1-001",
) -> tuple[Any, ...]:
    """Returns a cleaned worker row in schema column order."""
    return (
        location_id,
        establishment_id,
        worker_id,
        "1",
        "1",
        import_date,
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
    establishment_in_both_snapshots: bool | None = None,
) -> tuple[Any, ...]:
    """Returns a permanent worker row with the turnover flags appended.

    `establishment_in_both_snapshots` defaults to true where `is_leaver` is set
    and null otherwise; pass it explicitly for any other combination.
    """
    if establishment_in_both_snapshots is None and is_leaver is not None:
        establishment_in_both_snapshots = True
    return (
        *worker_row(worker_id, import_date, establishment_id=establishment_id),
        establishment_in_both_snapshots,
        is_leaver,
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
class EstablishmentFlagCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]  # (worker_id, import_date, flag, is_leaver)


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
            id="removes_workers_with_null_location_id",
            input_rows=[
                worker_row("1", date(2021, 1, 5), location_id="1-002"),
                worker_row("2", date(2021, 1, 5), location_id=None),
            ],
            expected_rows=[
                worker_row("1", date(2021, 1, 5), location_id="1-002"),
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
                worker_row("2", date(2022, 1, 5), establishment_id="1-001"),
                worker_row("1", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                flagged_row("1", date(2021, 1, 5), True, establishment_id="1-001"),
                flagged_row("2", date(2022, 1, 5), None, establishment_id="1-001"),
                flagged_row("1", date(2022, 1, 5), None, establishment_id="1-002"),
            ],
        ),
        AddIsLeaverFlagCase(
            id="leaves_is_leaver_null_when_establishment_missing_from_end_snapshot",
            input_rows=[
                worker_row("1", date(2021, 1, 5), establishment_id="1-001"),
                worker_row("2", date(2021, 1, 5), establishment_id="1-002"),
                worker_row("2", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                flagged_row(
                    "1",
                    date(2021, 1, 5),
                    None,
                    establishment_id="1-001",
                    establishment_in_both_snapshots=False,
                ),
                flagged_row("2", date(2021, 1, 5), False, establishment_id="1-002"),
                flagged_row("2", date(2022, 1, 5), None, establishment_id="1-002"),
            ],
        ),
        AddIsLeaverFlagCase(
            id="matches_establishment_in_end_snapshot_on_same_month_only",
            input_rows=[
                worker_row("1", date(2021, 1, 5), establishment_id="1-001"),
                worker_row("2", date(2022, 2, 5), establishment_id="1-001"),
                worker_row("3", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                flagged_row(
                    "1",
                    date(2021, 1, 5),
                    None,
                    establishment_id="1-001",
                    establishment_in_both_snapshots=False,
                ),
                flagged_row("2", date(2022, 2, 5), None, establishment_id="1-001"),
                flagged_row("3", date(2022, 1, 5), None, establishment_id="1-002"),
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

    establishment_in_both_snapshots_cases = [
        EstablishmentFlagCase(
            id="true_when_worker_stays_at_establishment",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2022, 1, 5)),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), True, False),
                ("1", date(2022, 1, 5), None, None),
            ],
        ),
        EstablishmentFlagCase(
            id="true_when_worker_leaves_but_establishment_has_another_worker",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("2", date(2022, 1, 5)),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), True, True),
                ("2", date(2022, 1, 5), None, None),
            ],
        ),
        EstablishmentFlagCase(
            id="false_when_establishment_missing_from_end_snapshot",
            input_rows=[
                worker_row("1", date(2021, 1, 5), establishment_id="1-001"),
                worker_row("2", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), False, None),
                ("2", date(2022, 1, 5), None, None),
            ],
        ),
        EstablishmentFlagCase(
            id="false_when_establishment_only_in_other_end_months",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("2", date(2022, 2, 5)),
                worker_row("3", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), False, None),
                ("2", date(2022, 2, 5), None, None),
                ("3", date(2022, 1, 5), None, None),
            ],
        ),
        EstablishmentFlagCase(
            id="flags_each_establishment_separately_in_same_month",
            input_rows=[
                worker_row("1", date(2021, 1, 5), establishment_id="1-001"),
                worker_row("2", date(2021, 1, 5), establishment_id="1-002"),
                worker_row("2", date(2022, 1, 5), establishment_id="1-002"),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), False, None),
                ("2", date(2021, 1, 5), True, False),
                ("2", date(2022, 1, 5), None, None),
            ],
        ),
        EstablishmentFlagCase(
            id="null_when_no_end_snapshot_exists",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("1", date(2021, 2, 5)),
                worker_row("1", date(2022, 1, 5)),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), True, False),
                ("1", date(2021, 2, 5), None, None),
                ("1", date(2022, 1, 5), None, None),
            ],
        ),
        EstablishmentFlagCase(
            id="keeps_one_row_per_worker_when_establishment_has_many_end_workers",
            input_rows=[
                worker_row("1", date(2021, 1, 5)),
                worker_row("2", date(2022, 1, 5)),
                worker_row("3", date(2022, 1, 5)),
            ],
            expected_rows=[
                ("1", date(2021, 1, 5), True, True),
                ("2", date(2022, 1, 5), None, None),
                ("3", date(2022, 1, 5), None, None),
            ],
        ),
    ]
