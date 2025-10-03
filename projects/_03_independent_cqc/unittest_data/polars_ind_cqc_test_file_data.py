from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class EstimateIndCQCFilledPostsByJobRoleUtilsData:
    aggregate_ascwds_worker_job_roles_per_establishment_rows = [
        ("101", "101", "102", "102"),
        (date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1), date(2025, 1, 1)),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.care_worker,
        ),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_rows = [
        ("101", "101", "101", "101", "101", "101", "102", "102", "102"),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
        ),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
        ),
        (1, None, None, None, 1, None, 2, None, None),
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
        ("1001", "1001", "1002"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_management,
            MainJobRoleLabels.care_worker,
        ),
        (10, 5, 20),
    ]
    expected_join_worker_to_estimates_dataframe_rows = [
        (
            "1-001",
            "1-001",
            "1-001",
            "1-002",
        ),
        (
            "1001",
            "1001",
            "1001",
            "1002",
        ),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 1),
        ),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_management,
            None,
            MainJobRoleLabels.care_worker,
        ),
        (10, 5, None, 20),
    ]
