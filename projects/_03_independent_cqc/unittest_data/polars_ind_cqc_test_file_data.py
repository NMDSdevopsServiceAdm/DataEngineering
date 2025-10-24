from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class PrepareJobRoleCountsUtilsData:
    aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "01"),
        ("20250101", "20250101"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "01"),
        ("20250101", "20250101"),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.senior_care_worker),
        (2, 0),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 2)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "02"),
        ("20250101", "20250102"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows = [
        ("101", "101", "101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 2)),
        ("2025", "2025", "2025", "2025"),
        ("01", "01", "01", "01"),
        ("01", "01", "02", "02"),
        ("20250101", "20250101", "20250102", "20250102"),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ),
        (1, 0, 1, 0),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows = [
        ("101", "102"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "01"),
        ("20250101", "20250101"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows = [
        ("101", "101", "102", "102"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        ("2025", "2025", "2025", "2025"),
        ("01", "01", "01", "01"),
        ("01", "01", "01", "01"),
        ("20250101", "20250101", "20250101", "20250101"),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ),
        (1, 0, 1, 0),
    ]


@dataclass
class EstimateIndCqcFilledPostsByJobRoleUtilsData:
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
