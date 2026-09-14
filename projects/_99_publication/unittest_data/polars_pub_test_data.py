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


@dataclass
class AddDispersionFilterTestCase:
    id: str
    column_names: list[str]
    from_date: date
    column_alias: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


CT_EMPLOYED_COLUMNS = [
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.ct_non_res_care_workers_employed_imputed,
]

add_dispersion_filter_test_cases = [
    AddDispersionFilterTestCase(
        id="false_only_for_the_location_swinging_beyond_two_standard_deviations",
        column_names=CT_EMPLOYED_COLUMNS,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_dispersion_filter_long_term,
        expected_data=[
            ("1-001", date(2021, 7, 1), 100.0, None, True),
            ("1-001", date(2022, 7, 1), 100.0, None, True),
            ("1-002", date(2021, 7, 1), 100.0, None, True),
            ("1-002", date(2022, 7, 1), 110.0, None, True),
            ("1-003", date(2021, 7, 1), 100.0, None, True),
            ("1-003", date(2022, 7, 1), 90.0, None, True),
            ("1-004", date(2021, 7, 1), 200.0, None, True),
            ("1-004", date(2022, 7, 1), 220.0, None, True),
            ("1-005", date(2021, 7, 1), 50.0, None, True),
            ("1-005", date(2022, 7, 1), 55.0, None, True),
            ("1-006", date(2021, 7, 1), 80.0, None, True),
            ("1-006", date(2022, 7, 1), 76.0, None, True),
            ("1-007", date(2021, 7, 1), 100.0, None, True),
            ("1-007", date(2022, 7, 1), 105.0, None, True),
            ("1-008", date(2021, 7, 1), 10.0, None, False),
            ("1-008", date(2022, 7, 1), 500.0, None, False),
        ],
    ),
    AddDispersionFilterTestCase(
        # 1-006's dispersion of 1.2 is outside the care home locations' own
        # boundaries, but inside the boundaries of the two columns pooled.
        id="scores_each_source_column_against_its_own_population_of_locations",
        column_names=CT_EMPLOYED_COLUMNS,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_dispersion_filter_long_term,
        expected_data=[
            ("1-001", date(2021, 7, 1), 100.0, None, True),
            ("1-001", date(2022, 7, 1), 100.0, None, True),
            ("1-002", date(2021, 7, 1), 100.0, None, True),
            ("1-002", date(2022, 7, 1), 100.0, None, True),
            ("1-003", date(2021, 7, 1), 100.0, None, True),
            ("1-003", date(2022, 7, 1), 100.0, None, True),
            ("1-004", date(2021, 7, 1), 100.0, None, True),
            ("1-004", date(2022, 7, 1), 100.0, None, True),
            ("1-005", date(2021, 7, 1), 100.0, None, True),
            ("1-005", date(2022, 7, 1), 100.0, None, True),
            ("1-006", date(2021, 7, 1), 10.0, None, False),
            ("1-006", date(2022, 7, 1), 40.0, None, False),
            ("1-007", date(2021, 7, 1), None, 10.0, True),
            ("1-007", date(2022, 7, 1), None, 10.0, True),
            ("1-008", date(2021, 7, 1), None, 10.0, True),
            ("1-008", date(2022, 7, 1), None, 20.0, True),
            ("1-009", date(2021, 7, 1), None, 10.0, True),
            ("1-009", date(2022, 7, 1), None, 30.0, True),
            ("1-010", date(2021, 7, 1), None, 10.0, True),
            ("1-010", date(2022, 7, 1), None, 40.0, True),
            ("1-011", date(2021, 7, 1), None, 10.0, True),
            ("1-011", date(2022, 7, 1), None, 60.0, True),
            ("1-012", date(2021, 7, 1), None, 10.0, True),
            ("1-012", date(2022, 7, 1), None, 100.0, True),
        ],
    ),
    AddDispersionFilterTestCase(
        # 1-006 repeats one import date over three job role rows. Averaged over
        # rows its dispersion would be 1.714 not 1.2, putting it outside the
        # boundaries the other five locations set.
        id="repeated_job_role_rows_do_not_bias_the_mean",
        column_names=CT_EMPLOYED_COLUMNS,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_dispersion_filter_long_term,
        expected_data=[
            ("1-001", date(2021, 7, 1), 10.0, None, True),
            ("1-001", date(2022, 7, 1), 40.0, None, True),
            ("1-002", date(2021, 7, 1), 10.0, None, True),
            ("1-002", date(2022, 7, 1), 40.0, None, True),
            ("1-003", date(2021, 7, 1), 10.0, None, True),
            ("1-003", date(2022, 7, 1), 40.0, None, True),
            ("1-004", date(2021, 7, 1), 10.0, None, True),
            ("1-004", date(2022, 7, 1), 40.0, None, True),
            ("1-005", date(2021, 7, 1), 10.0, None, True),
            ("1-005", date(2022, 7, 1), 40.0, None, True),
            ("1-006", date(2021, 7, 1), 10.0, None, True),
            ("1-006", date(2021, 7, 1), 10.0, None, True),
            ("1-006", date(2021, 7, 1), 10.0, None, True),
            ("1-006", date(2022, 7, 1), 40.0, None, True),
        ],
    ),
    AddDispersionFilterTestCase(
        id="false_for_a_location_with_no_data_in_the_window",
        column_names=CT_EMPLOYED_COLUMNS,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_dispersion_filter_long_term,
        expected_data=[
            ("1-001", date(2021, 7, 1), 10.0, None, True),
            ("1-001", date(2022, 7, 1), 40.0, None, True),
            ("1-002", date(2021, 7, 1), 10.0, None, True),
            ("1-002", date(2022, 7, 1), 40.0, None, True),
            ("1-003", date(2021, 7, 1), 10.0, None, True),
            ("1-003", date(2022, 7, 1), 40.0, None, True),
            ("1-004", date(2021, 7, 1), 10.0, None, True),
            ("1-004", date(2022, 7, 1), 40.0, None, True),
            ("1-005", date(2021, 7, 1), 10.0, None, True),
            ("1-005", date(2022, 7, 1), 40.0, None, True),
            ("1-006", date(2021, 7, 1), 10.0, None, True),
            ("1-006", date(2022, 7, 1), 40.0, None, True),
            ("1-007", date(2021, 7, 1), None, None, False),
            ("1-008", date(2020, 7, 1), 10.0, None, False),
            ("1-008", date(2020, 7, 1), 40.0, None, False),
        ],
    ),
    AddDispersionFilterTestCase(
        # A zero mean gives an undefined dispersion, which must not propagate
        # into the boundaries and fail every other location.
        id="false_for_a_location_reporting_zero_at_every_import_date",
        column_names=CT_EMPLOYED_COLUMNS,
        from_date=date(2021, 7, 1),
        column_alias=Pub.ct_dispersion_filter_long_term,
        expected_data=[
            ("1-001", date(2021, 7, 1), 10.0, None, True),
            ("1-001", date(2022, 7, 1), 40.0, None, True),
            ("1-002", date(2021, 7, 1), 10.0, None, True),
            ("1-002", date(2022, 7, 1), 40.0, None, True),
            ("1-003", date(2021, 7, 1), 10.0, None, True),
            ("1-003", date(2022, 7, 1), 40.0, None, True),
            ("1-004", date(2021, 7, 1), 10.0, None, True),
            ("1-004", date(2022, 7, 1), 40.0, None, True),
            ("1-005", date(2021, 7, 1), 10.0, None, True),
            ("1-005", date(2022, 7, 1), 40.0, None, True),
            ("1-006", date(2021, 7, 1), 10.0, None, True),
            ("1-006", date(2022, 7, 1), 40.0, None, True),
            ("1-007", date(2021, 7, 1), 0.0, None, False),
            ("1-007", date(2022, 7, 1), 0.0, None, False),
        ],
    ),
    AddDispersionFilterTestCase(
        # 1-006's spike is before from_date. Counting it would give a dispersion
        # of 2.25 and fail the location.
        id="ignores_import_dates_before_from_date",
        column_names=CT_EMPLOYED_COLUMNS,
        from_date=date(2025, 4, 1),
        column_alias=Pub.ct_dispersion_filter_medium_term,
        expected_data=[
            ("1-001", date(2024, 4, 1), 100.0, None, True),
            ("1-001", date(2025, 4, 1), 100.0, None, True),
            ("1-001", date(2026, 4, 1), 100.0, None, True),
            ("1-002", date(2024, 4, 1), 100.0, None, True),
            ("1-002", date(2025, 4, 1), 100.0, None, True),
            ("1-002", date(2026, 4, 1), 100.0, None, True),
            ("1-003", date(2024, 4, 1), 100.0, None, True),
            ("1-003", date(2025, 4, 1), 100.0, None, True),
            ("1-003", date(2026, 4, 1), 100.0, None, True),
            ("1-004", date(2024, 4, 1), 100.0, None, True),
            ("1-004", date(2025, 4, 1), 100.0, None, True),
            ("1-004", date(2026, 4, 1), 100.0, None, True),
            ("1-005", date(2024, 4, 1), 100.0, None, True),
            ("1-005", date(2025, 4, 1), 100.0, None, True),
            ("1-005", date(2026, 4, 1), 100.0, None, True),
            ("1-006", date(2024, 4, 1), 1000.0, None, True),
            ("1-006", date(2025, 4, 1), 100.0, None, True),
            ("1-006", date(2026, 4, 1), 100.0, None, True),
        ],
    ),
]
