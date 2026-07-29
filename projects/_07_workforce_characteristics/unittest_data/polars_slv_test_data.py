from dataclasses import dataclass
from datetime import date

import pytest

from projects._07_workforce_characteristics.unittest_data.polars_slv_test_schemas import (
    REAL_JOB_ROLE_CODES,
    PivotJobRoleColsToRowsSchemas as Schemas,
)


@dataclass
class PivotJobRoleColsToRowsCase:
    id: str
    input_schema: dict
    input_rows: list[tuple]
    expected_rows: list[tuple]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


realistic_input_rows = [
    (
        "601",
        date(2025, 6, 1),
        *[value for _ in REAL_JOB_ROLE_CODES for value in (1, 2, 3, 4)],
    )
]
realistic_expected_rows = [
    ("601", date(2025, 6, 1), str(int(code)), 1, 2, 3, 4)
    for code in REAL_JOB_ROLE_CODES
]


@dataclass
class PivotJobRoleColsToRowsData:
    no_hardcoded_code_count_cases = [
        PivotJobRoleColsToRowsCase(
            id="handles_small_synthetic_code_set_across_multiple_establishments_and_dates",
            input_schema=Schemas.synthetic_input_schema,
            input_rows=[
                ("101", date(2024, 1, 1), 5, 1, 0, 2, 8, 2, 1, 0, 3, 0, 0, 1),
                ("102", date(2024, 2, 1), 10, 3, 2, 0, 4, 1, 0, 3, 6, 2, 1, 2),
            ],
            expected_rows=[
                ("101", date(2024, 1, 1), "2", 5, 1, 0, 2),
                ("101", date(2024, 1, 1), "10", 8, 2, 1, 0),
                ("101", date(2024, 1, 1), "20", 3, 0, 0, 1),
                ("102", date(2024, 2, 1), "2", 10, 3, 2, 0),
                ("102", date(2024, 2, 1), "10", 4, 1, 0, 3),
                ("102", date(2024, 2, 1), "20", 6, 2, 1, 2),
            ],
        ),
        PivotJobRoleColsToRowsCase(
            id="handles_realistic_14_code_production_set_with_real_code_shapes",
            input_schema=Schemas.realistic_input_schema,
            input_rows=realistic_input_rows,
            expected_rows=realistic_expected_rows,
        ),
    ]

    partial_null_input_rows = [
        ("201", date(2025, 1, 1), None, 3, 1, 2, 7, 2, 0, 1),
    ]
    partial_null_expected_rows = [
        ("201", date(2025, 1, 1), "5", None, 3, 1, 2),
        ("201", date(2025, 1, 1), "6", 7, 2, 0, 1),
    ]

    all_null_input_rows = [
        ("301", date(2025, 3, 1), None, None, None, None),
    ]
    all_null_expected_rows = [
        ("301", date(2025, 3, 1), "9", None, None, None, None),
    ]

    zero_codes_input_rows = [
        ("401", date(2025, 4, 1), "South West"),
    ]

    column_scope_input_rows = [
        ("501", date(2025, 5, 1), 12, 4, 1, 3, "South West", "Local Authority"),
    ]
    column_scope_expected_rows = [
        ("501", date(2025, 5, 1), "1", 12, 4, 1, 3),
    ]
