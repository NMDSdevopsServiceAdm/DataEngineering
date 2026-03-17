"""This module defines test data and schema for the utils tests."""

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl
import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
)


@dataclass
class TestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


rolling_sum_expected_schema = {
    IndCQC.location_id: pl.String,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.primary_service_type: pl.String,
    IndCQC.main_job_role_clean_labelled: pl.String,
    IndCQC.imputed_ascwds_job_role_counts: pl.Float64,
    IndCQC.ascwds_job_role_rolling_sum: pl.Float64,
}


rolling_sum_test_cases = [
    TestCase(
        id="when_one_primary_service_present",
        data=[
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "care_worker", 1.0, 1.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "registered_nurse", 2.0, 2.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "senior_care_worker", 3.0, 3.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "senior_management", 4.0, 4.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "care_worker", None, 1.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "registered_nurse", None, 2.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "senior_care_worker", None, 3.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "senior_management", None, 4.0),
            ("1000", date(2024, 1, 3), "care_home_with_nursing", "care_worker", 5.0, 6.0),
            ("1000", date(2024, 1, 3), "care_home_with_nursing", "registered_nurse", 6.0, 8.0),
            ("1000", date(2024, 1, 3), "care_home_with_nursing", "senior_care_worker", 7.0, 10.0),
            ("1000", date(2024, 1, 3), "care_home_with_nursing", "senior_management", 8.0, 12.0),
        ],
    ),
    TestCase(
        id="when_multiple_primary_services_present",
        data=[
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "care_worker", 1.0, 1.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "registered_nurse", 2.0, 2.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "senior_care_worker", 3.0, 3.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "senior_management", 4.0, 4.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "care_worker", 5.0, 6.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "registered_nurse", 6.0, 8.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "senior_care_worker", 7.0, 10.0),
            ("1000", date(2024, 1, 2), "care_home_with_nursing", "senior_management", 8.0, 12.0),
            ("1000", date(2024, 1, 1), "care_home_only", "care_worker", 11.0, 11.0),
            ("1000", date(2024, 1, 1), "care_home_only", "registered_nurse", 12.0, 12.0),
            ("1000", date(2024, 1, 1), "care_home_only", "senior_care_worker", 13.0, 13.0),
            ("1000", date(2024, 1, 1), "care_home_only", "senior_management", 14.0, 14.0),
            ("1000", date(2024, 1, 2), "care_home_only", "care_worker", 15.0, 26.0),
            ("1000", date(2024, 1, 2), "care_home_only", "registered_nurse", 16.0, 28.0),
            ("1000", date(2024, 1, 2), "care_home_only", "senior_care_worker", 17.0, 30.0),
            ("1000", date(2024, 1, 2), "care_home_only", "senior_management", 18.0, 32.0),
        ],
    ),
    TestCase(
        id="when_days_not_within_rolling_window",
        data=[
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "care_worker", 1.0, 1.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "registered_nurse", 2.0, 2.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "senior_care_worker", 3.0, 3.0),
            ("1000", date(2024, 1, 1), "care_home_with_nursing", "senior_management", 4.0, 4.0),
            ("1000", date(2024, 7, 5), "care_home_with_nursing", "care_worker", 5.0, 5.0),
            ("1000", date(2024, 7, 5), "care_home_with_nursing", "registered_nurse", 6.0, 6.0),
            ("1000", date(2024, 7, 5), "care_home_with_nursing", "senior_care_worker", 7.0, 7.0),
            ("1000", date(2024, 7, 5), "care_home_with_nursing", "senior_management", 8.0, 8.0),
        ],
    ),
]  # fmt: skip

managerial_adjustment_core_schema = {
    IndCQC.location_id: pl.String,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.registered_manager_names: pl.List,
    IndCQC.main_job_role_clean_labelled: pl.String,
    IndCQC.estimate_filled_posts_by_job_role: pl.Float64,
}

managerial_adjustment_expected_schema = managerial_adjustment_core_schema | {
    # These output columns will be used by different tests.
    "diff": pl.Float64,
    "proportions": pl.Float64,
    "adjusted_estimates": pl.Float64,
}

managerial_adjustment_test_cases = [
    TestCase(
        id="1_rm_count_with_estimate_>_1",
        data=[
            ("1-001", date(2025, 1, 1), ["Sarah"], MainJobRoleLabels.care_worker,         10, 4,   None, 10),
            ("1-001", date(2025, 1, 1), ["Sarah"], MainJobRoleLabels.senior_care_worker,  15, 4,   None, 15),
            ("1-001", date(2025, 1, 1), ["Sarah"], MainJobRoleLabels.supervisor,          20, 4, 0.4444, 21.778),
            ("1-001", date(2025, 1, 1), ["Sarah"], MainJobRoleLabels.team_leader,         25, 4, 0.5556, 27.222),
            ("1-001", date(2025, 1, 1), ["Sarah"], MainJobRoleLabels.registered_manager,   5, 4,   None, 1),
        ],
    ),
    TestCase(
        id="0_rm_count_with_estimate_>_1",
        data=[
            ("1-002", date(2025, 1, 1), [], MainJobRoleLabels.care_worker,        10, 5,   None, 10),
            ("1-002", date(2025, 1, 1), [], MainJobRoleLabels.senior_care_worker, 15, 5,   None, 15),
            ("1-002", date(2025, 1, 1), [], MainJobRoleLabels.supervisor,         20, 5, 0.4444, 22.222),
            ("1-002", date(2025, 1, 1), [], MainJobRoleLabels.team_leader,        25, 5, 0.5556, 27.778),
            ("1-002", date(2025, 1, 1), [], MainJobRoleLabels.registered_manager,  5, 5,   None, 0),
        ],
    ),
    TestCase(
        id="1_rm_count_with_estimate_==_0",
        data=[
            ("1-001", date(2025, 2, 1), ["James"], MainJobRoleLabels.care_worker,         10, -1,   None, 10),
            ("1-001", date(2025, 2, 1), ["James"], MainJobRoleLabels.senior_care_worker,  15, -1,   None, 15),
            ("1-001", date(2025, 2, 1), ["James"], MainJobRoleLabels.supervisor,          20, -1, 0.4444, 19.556),
            ("1-001", date(2025, 2, 1), ["James"], MainJobRoleLabels.team_leader,         25, -1, 0.5556, 24.444),
            ("1-001", date(2025, 2, 1), ["James"], MainJobRoleLabels.registered_manager,   0, -1,   None, 1),
        ],
    ),
    TestCase(
        id="1_rm_count_with_manager_roles_sum_<_1",
        data=[
            ("1-002", date(2025, 2, 1), ["James"], MainJobRoleLabels.care_worker,         10, -1,   None, 10.0),
            ("1-002", date(2025, 2, 1), ["James"], MainJobRoleLabels.senior_care_worker,  15, -1,   None, 15.0),
            ("1-002", date(2025, 2, 1), ["James"], MainJobRoleLabels.supervisor,         0.2, -1, 0.6667, 0),
            ("1-002", date(2025, 2, 1), ["James"], MainJobRoleLabels.team_leader,        0.1, -1, 0.3333, 0),
            ("1-002", date(2025, 2, 1), ["James"], MainJobRoleLabels.registered_manager,   0, -1,   None, 1.0),
        ],
    ),
    TestCase(
        id="1_rm_count_with_manager_roles_sum_==_0_check_even_distribution",
        data=[
            ("1-005", date(2024, 1, 1), ["James"], MainJobRoleLabels.care_worker,         10, 2,   None, 10.0),
            ("1-005", date(2024, 1, 1), ["James"], MainJobRoleLabels.senior_care_worker,  15, 2,   None, 15.0),
            ("1-005", date(2024, 1, 1), ["James"], MainJobRoleLabels.supervisor,           0, 2,    0.5, 1.0),
            ("1-005", date(2024, 1, 1), ["James"], MainJobRoleLabels.team_leader,          0, 2,    0.5, 1.0),
            ("1-005", date(2024, 1, 1), ["James"], MainJobRoleLabels.registered_manager,   3, 2,   None, 1.0),
        ],
    ),
    TestCase(
        id="1_rm_count_multiple_manager_names_with_estimate_>_1",
        data=[
            ("1-008", date(2025, 1, 1), ["Sarah", "James"], MainJobRoleLabels.care_worker,         10, 4,   None, 10),
            ("1-008", date(2025, 1, 1), ["Sarah", "James"], MainJobRoleLabels.senior_care_worker,  15, 4,   None, 15),
            ("1-008", date(2025, 1, 1), ["Sarah", "James"], MainJobRoleLabels.supervisor,          20, 4, 0.4444, 21.778),
            ("1-008", date(2025, 1, 1), ["Sarah", "James"], MainJobRoleLabels.team_leader,         25, 4, 0.5556, 27.222),
            ("1-008", date(2025, 1, 1), ["Sarah", "James"], MainJobRoleLabels.registered_manager,   5, 4,   None, 1),
        ],
    ),
]  # fmt: skip
