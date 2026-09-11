from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub


@dataclass
class HasContinuousDataSinceDateTestCase:
    id: str
    column_name: str
    from_date: date
    column_alias: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


has_continuous_data_since_date_test_cases = [
    HasContinuousDataSinceDateTestCase(
        id="true_when_no_nulls_in_the_checked_column_since_cutoff",
        column_name=IndCQC.ct_care_home_total_employed_imputed,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_has_data_long_term,
        expected_data=[
            ("1-001", date(2021, 7, 1), 5.0, None, True),
            ("1-001", date(2022, 7, 1), 6.0, None, True),
        ],
    ),
    HasContinuousDataSinceDateTestCase(
        id="false_when_the_checked_column_has_a_null_since_cutoff",
        column_name=IndCQC.ct_care_home_total_employed_imputed,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_has_data_long_term,
        expected_data=[
            ("1-002", date(2021, 7, 1), None, 3.0, False),
            ("1-002", date(2022, 7, 1), 5.0, 4.0, False),
        ],
    ),
    HasContinuousDataSinceDateTestCase(
        id="false_for_location_with_no_rows_on_or_after_cutoff",
        column_name=IndCQC.ct_care_home_total_employed_imputed,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_has_data_long_term,
        expected_data=[
            ("1-003", date(2020, 7, 1), 5.0, 5.0, False),
        ],
    ),
    HasContinuousDataSinceDateTestCase(
        id="honours_the_column_name_argument",
        column_name=IndCQC.ct_non_res_care_workers_employed_imputed,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_has_data_long_term,
        expected_data=[
            ("1-004", date(2021, 7, 1), None, 3.0, True),
            ("1-004", date(2022, 7, 1), None, 4.0, True),
        ],
    ),
    HasContinuousDataSinceDateTestCase(
        id="honours_a_non_default_column_alias",
        column_name=IndCQC.ct_care_home_total_employed_imputed,
        from_date=date(2025, 4, 1),
        column_alias=Pub.ct_has_data_medium_term,
        expected_data=[
            ("1-005", date(2025, 4, 1), 5.0, None, True),
        ],
    ),
    HasContinuousDataSinceDateTestCase(
        id="false_for_location_missing_a_period_another_location_has",
        column_name=IndCQC.ct_care_home_total_employed_imputed,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_has_data_long_term,
        expected_data=[
            ("1-006", date(2021, 7, 1), 5.0, None, False),
            ("1-006", date(2022, 7, 1), 5.0, None, False),
            ("1-007", date(2021, 7, 1), 5.0, None, True),
            ("1-007", date(2022, 7, 1), 5.0, None, True),
            ("1-007", date(2023, 7, 1), 5.0, None, True),
        ],
    ),
    HasContinuousDataSinceDateTestCase(
        id="duplicate_rows_at_the_same_date_do_not_inflate_the_count",
        column_name=IndCQC.ct_care_home_total_employed_imputed,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_has_data_long_term,
        expected_data=[
            ("1-008", date(2021, 7, 1), None, None, True),
            ("1-008", date(2021, 7, 1), 5.0, None, True),
            ("1-008", date(2022, 7, 1), 6.0, None, True),
        ],
    ),
]
