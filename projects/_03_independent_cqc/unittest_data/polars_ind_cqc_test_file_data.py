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
        (1, 0, 0, 0, 1, 0, 2, 0, 0),
    ]
