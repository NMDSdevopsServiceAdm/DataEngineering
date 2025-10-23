from dataclasses import dataclass
from datetime import date

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class PrepareJobRoleCountsUtilsData:
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
        (
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
        ),
        (
            "01",
            "01",
            "01",
            "01",
            "01",
        ),
        (
            "01",
            "01",
            "02",
            "01",
            "01",
        ),
        (
            "20250101",
            "20250101",
            "20250102",
            "20250101",
            "20250101",
        ),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_rows = [
        (
            "101",
            "101",
            "101",
            "101",
            "101",
            "101",
            "101",
            "101",
            "102",
            "102",
            "102",
            "102",
        ),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
        ),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
            MainJobRoleLabels.social_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
            MainJobRoleLabels.social_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
            MainJobRoleLabels.social_worker,
        ),
        (1, 1, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0),
        (
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
            "2025",
        ),
        ("01", "01", "01", "01", "01", "01", "01", "01", "01", "01", "01", "01"),
        ("01", "01", "01", "01", "02", "02", "02", "02", "01", "01", "01", "01"),
        (
            "20250101",
            "20250101",
            "20250101",
            "20250101",
            "20250102",
            "20250102",
            "20250102",
            "20250102",
            "20250101",
            "20250101",
            "20250101",
            "20250101",
        ),
    ]

    create_job_role_ratios_rows = [
        ("1001", "1001", "1001", "1001", "1002", "1002"),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 2),
        ),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ),
        (5, 5, 4, 6, 3, 7),
    ]
    expected_create_job_role_ratios_rows = create_job_role_ratios_rows + [
        (0.5, 0.5, 0.4, 0.6, 0.3, 0.7)
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
