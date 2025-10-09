from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class EstimateIndCQCFilledPostsByJobRoleUtilsData:
    aggregate_ascwds_worker_job_roles_per_establishment_rows = [
        ("101", "101", "101", "102", "102"),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 1),
            date(2025, 1, 1),
        ),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.care_worker,
        ),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_rows = [
        ("101", "101", "102"),
        (
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 1),
        ),
        (1, 0, 2),
        (1, 1, 0),
        (0, 0, 0),
    ]

    estimates_df_before_join_rows = [
        (
            "1-001",
            "1-001",
            "1-002",
        ),
        (
            "1001",
            "1001",
            "1002",
        ),
        (date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)),
    ]
    worker_df_before_join_rows = [
        ("1001", "1002"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (10, 0),
        (20, 0),
        (30, 0),
    ]
    expected_join_worker_to_estimates_dataframe_rows = [
        (
            "1-001",
            "1-001",
            "1-002",
        ),
        (
            "1001",
            "1001",
            "1002",
        ),
        (
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 1),
        ),
        (10, None, 0),
        (20, None, 0),
        (30, None, 0),
    ]
