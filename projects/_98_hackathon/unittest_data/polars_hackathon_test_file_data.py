from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat


def worker_row(
    worker_id: str, import_date: date, employment_status_label: str
) -> tuple[Any, ...]:
    """Returns a cleaned worker row in schema column order."""
    return (
        "1-001",
        "1-001",
        worker_id,
        "1",
        "1",
        import_date,
        "1",
        "care_worker",
        "1",
        employment_status_label,
    )


@dataclass
class FilterWorkerTurnoverDataCase:
    id: str
    input_rows: list[tuple[Any, ...]]
    expected_rows: list[tuple[Any, ...]]


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
