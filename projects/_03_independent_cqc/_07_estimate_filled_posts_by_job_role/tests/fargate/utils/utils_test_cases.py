"""This module defines test data and schema for the utils tests."""

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class TestCase:
    id: str
    data: list[Any]


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
