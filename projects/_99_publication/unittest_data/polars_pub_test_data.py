from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostic_thresholds as DT,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PrimaryServiceType


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
        # 1-006 has only one import date in the window, so its trivial zero
        # dispersion must not be treated as evidence of stability - the flat
        # population here means it would otherwise land exactly on the
        # boundary and pass.
        id="false_for_a_location_with_only_one_import_date_in_the_window",
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
            ("1-006", date(2021, 7, 1), 50.0, None, False),
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


@dataclass
class AggregateToPublicationRowsTestCase:
    id: str
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_ALL_TRUE_FILTERS = (True, True, True, True, True, True, True)
_NURSE_LONDON_CARE_HOME = ("Registered nurse", "London", "Care home service")

aggregate_to_publication_rows_test_cases = [
    AggregateToPublicationRowsTestCase(
        id="publication_columns_include_rows_that_assessment_columns_exclude",
        input_data=[
            (
                "1-001",
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-002",
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                20.0,
                8.0,
                False,  # consistent_service
                True,
                True,
                True,
                True,
                True,
                True,
            ),
        ],
        expected_data=[
            (
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                30.0,
                2,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
            ),
        ],
    ),
    AggregateToPublicationRowsTestCase(
        # 1-003 passes the long term filters but fails medium and short term -
        # each term's assessment columns must reflect only its own filters.
        id="each_term_filters_independently_of_the_other_two",
        input_data=[
            (
                "1-003",
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                5.0,
                True,  # consistent_service
                True,  # ct_has_data_long_term
                False,  # ct_has_data_medium_term
                True,  # ct_has_data_short_term
                True,  # ct_dispersion_filter_long_term
                True,  # ct_dispersion_filter_medium_term
                False,  # ct_dispersion_filter_short_term
            ),
        ],
        expected_data=[
            (
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                1,
                10.0,
                1,
                5.0,
                0.0,
                0,
                0.0,
                0.0,
                0,
                0.0,
            ),
        ],
    ),
    AggregateToPublicationRowsTestCase(
        # Both rows are the same location - publication_locationid_count must
        # count distinct locations, not rows, while filled posts still sum
        # across both.
        id="counts_distinct_locations_not_rows",
        input_data=[
            (
                "1-004",
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-004",
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                15.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
        ],
        expected_data=[
            (
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                25.0,
                1,
                25.0,
                1,
                10.0,
                25.0,
                1,
                10.0,
                25.0,
                1,
                10.0,
            ),
        ],
    ),
    AggregateToPublicationRowsTestCase(
        # Same location and identical on every column except the one grouping
        # key each row changes in turn - each row must stay its own output
        # row, proving the group_by is keyed on all four columns, not just
        # whichever one two rows happen to share.
        id="rows_are_kept_separate_when_any_single_grouping_key_differs",
        input_data=[
            (
                "1-005",
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-005",
                date(2026, 4, 1),  # differs: import date
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-005",
                date(2025, 4, 1),
                "Care worker",  # differs: job role
                "London",
                "Care home service",
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-005",
                date(2025, 4, 1),
                "Registered nurse",
                "South West",  # differs: region
                "Care home service",
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-005",
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "Non-residential service",  # differs: service type
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
        ],
        expected_data=[
            (
                date(2025, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                1,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
            ),
            (
                date(2026, 4, 1),
                *_NURSE_LONDON_CARE_HOME,
                10.0,
                1,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "London",
                "Care home service",
                10.0,
                1,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "South West",
                "Care home service",
                10.0,
                1,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "Non-residential service",
                10.0,
                1,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
                10.0,
                1,
                5.0,
            ),
        ],
    ),
]


def _all_terms_metrics(
    filled_posts: float, locationid_count: int, ct_total_employed: float
) -> tuple:
    """Publication + long/medium/short assessment metrics, identical across
    all three terms since every input row's filters pass in these tests."""
    return (
        filled_posts,
        locationid_count,
        filled_posts,
        locationid_count,
        ct_total_employed,
        filled_posts,
        locationid_count,
        ct_total_employed,
        filled_posts,
        locationid_count,
        ct_total_employed,
    )


@dataclass
class AddRowsForPublicationGroupsTestCase:
    id: str
    input_data: list[Any]
    expected_row_count: int
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_CARE_HOME_WITH_NURSING = PrimaryServiceType.care_home_with_nursing
_NON_RESIDENTIAL = PrimaryServiceType.non_residential

add_rows_for_publication_groups_test_cases = [
    AddRowsForPublicationGroupsTestCase(
        # A multi-job-role location counts once for "All job roles", not per role.
        id="all_job_roles_row_counts_a_multi_job_role_location_once_not_per_role",
        input_data=[
            (
                "1-001",
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _CARE_HOME_WITH_NURSING,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "1-001",
                date(2025, 4, 1),
                "Care worker",
                "London",
                _CARE_HOME_WITH_NURSING,
                20.0,
                8.0,
                *_ALL_TRUE_FILTERS,
            ),
        ],
        # 3 job roles (incl. rollup) x 2 regions (incl. England) x 3 service
        # types (incl. both rollups) = 18 rows; only the row that proves the
        # count doesn't double is worth spelling out.
        expected_row_count=18,
        expected_data=[
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
        ],
    ),
    AddRowsForPublicationGroupsTestCase(
        # "All CQC care homes" must exclude non-residential locations.
        id="all_cqc_care_homes_excludes_non_residential_locations",
        input_data=[
            (
                "2-001",
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _CARE_HOME_WITH_NURSING,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "2-002",
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _NON_RESIDENTIAL,
                15.0,
                6.0,
                *_ALL_TRUE_FILTERS,
            ),
        ],
        # 2 job roles (incl. rollup) x 2 regions (incl. England) x 4 service
        # types (both real, plus both rollups) = 16 rows; only the pair that
        # proves the inclusion/exclusion split is worth spelling out.
        expected_row_count=16,
        expected_data=[
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC locations",
                *_all_terms_metrics(25.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
        ],
    ),
    AddRowsForPublicationGroupsTestCase(
        # England must sum two real regions once each, not double-count.
        id="england_row_sums_across_multiple_real_regions_without_double_counting",
        input_data=[
            (
                "3-001",
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _CARE_HOME_WITH_NURSING,
                10.0,
                5.0,
                *_ALL_TRUE_FILTERS,
            ),
            (
                "3-002",
                date(2025, 4, 1),
                "Registered nurse",
                "South West",
                _CARE_HOME_WITH_NURSING,
                12.0,
                6.0,
                *_ALL_TRUE_FILTERS,
            ),
        ],
        # 2 job roles (incl. rollup) x 3 regions (incl. England) x 3 service
        # types (incl. both rollups) = 18 rows; only the England row that
        # proves the two real regions are summed once each is worth
        # spelling out: 22/2/11 = London (10/1/5) + South West (12/1/6).
        expected_row_count=18,
        expected_data=[
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(22.0, 2, 11.0),
            ),
        ],
    ),
]


@dataclass
class CalcPercChangeBetweenRowsTestCase:
    id: str
    column_name: str
    from_date: date
    group_columns: list[str]
    column_alias: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


@dataclass
class CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase:
    id: str
    column_name: str
    from_date: date
    group_columns: list[str]
    column_alias: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


PERC_CHANGE_GROUP_COLUMNS = [
    IndCQC.main_job_role_clean_labelled,
    IndCQC.current_region,
    IndCQC.primary_service_type,
]
_CARE_WORKER_SOUTH_WEST_NON_RES = (
    "Care worker",
    "South West",
    "Non-residential service",
)

calc_perc_change_between_rows_test_cases = [
    CalcPercChangeBetweenRowsTestCase(
        id="change_is_measured_against_the_previous_period_in_the_group",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, -0.5),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        id="periods_before_from_date_are_null",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2024, 4, 1), *_NURSE_LONDON_CARE_HOME, 999.0, None, None),
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        id="null_for_every_period_when_none_are_in_the_window",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2023, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2024, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, None),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        id="null_when_the_previous_period_is_zero",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 0.0, None, None),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 50.0, None, None),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        id="a_period_dropping_to_zero_is_a_full_decrease_not_a_null",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 0.0, None, -1.0),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        # No row at all for 2026-04-01 - the comparison silently spans the gap
        # to the last present period rather than treating it as a missing value.
        id="compares_with_the_last_present_period_when_one_is_missing",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        # Rows are deliberately out of date order within each group, so this
        # only passes if the window genuinely orders by import date rather
        # than relying on physical row order.
        id="groups_do_not_affect_each_other",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_long_term,
        expected_data=[
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
            (date(2027, 4, 1), *_CARE_WORKER_SOUTH_WEST_NON_RES, 25.0, None, -0.5),
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2026, 4, 1), *_CARE_WORKER_SOUTH_WEST_NON_RES, 50.0, None, None),
        ],
    ),
    CalcPercChangeBetweenRowsTestCase(
        id="honours_the_column_name_from_date_and_alias_arguments",
        column_name=Pub.assessment_ct_total_employed_medium_term,
        from_date=date(2026, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_period_perc_change_medium_term,
        expected_data=[
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, None, 40.0, None),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, None, 60.0, 0.5),
        ],
    ),
]

calc_perc_change_cumulative_from_given_period_onwards_test_cases = [
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        id="change_is_measured_against_the_first_period_in_the_window",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, 0.0),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, 400.0, None, 3.0),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        id="baseline_ignores_periods_before_from_date",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2024, 4, 1), *_NURSE_LONDON_CARE_HOME, 50.0, None, None),
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, 0.0),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        id="null_for_every_period_when_none_are_in_the_window",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2023, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
            (date(2024, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, None),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        id="null_when_the_baseline_period_is_zero",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 0.0, None, None),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 50.0, None, None),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, None),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        id="a_period_dropping_to_zero_is_a_full_decrease_not_a_null",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, 0.0),
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 0.0, None, -1.0),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        # No row at all for 2026-04-01 - the baseline stays anchored to the
        # first in-window period regardless of the gap.
        id="baseline_is_unaffected_by_a_missing_period",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, 0.0),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        # Rows are deliberately out of date order within each group, so this
        # only passes if the baseline genuinely orders by import date rather
        # than relying on physical row order.
        id="groups_do_not_affect_each_other",
        column_name=Pub.assessment_ct_total_employed_long_term,
        from_date=date(2025, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_long_term,
        expected_data=[
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, 200.0, None, 1.0),
            (date(2027, 4, 1), *_CARE_WORKER_SOUTH_WEST_NON_RES, 25.0, None, -0.5),
            (date(2025, 4, 1), *_NURSE_LONDON_CARE_HOME, 100.0, None, 0.0),
            (date(2026, 4, 1), *_CARE_WORKER_SOUTH_WEST_NON_RES, 50.0, None, 0.0),
        ],
    ),
    CalcPercChangeCumulativeFromGivenPeriodOnwardsTestCase(
        id="honours_the_column_name_from_date_and_alias_arguments",
        column_name=Pub.assessment_ct_total_employed_medium_term,
        from_date=date(2026, 4, 1),
        group_columns=PERC_CHANGE_GROUP_COLUMNS,
        column_alias=Pub.assessment_ct_cumulative_perc_change_medium_term,
        expected_data=[
            (date(2026, 4, 1), *_NURSE_LONDON_CARE_HOME, None, 40.0, 0.0),
            (date(2027, 4, 1), *_NURSE_LONDON_CARE_HOME, None, 60.0, 0.5),
        ],
    ),
]


@dataclass
class FormatLargeNumberTestCase:
    id: str
    column_name: str
    column_alias: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


format_large_number_test_cases = [
    FormatLargeNumberTestCase(
        id="value_under_a_thousand_has_no_separator",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(500.0, "500")],
    ),
    FormatLargeNumberTestCase(
        id="value_in_the_thousands_gets_one_comma",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(800000.0, "800,000")],
    ),
    FormatLargeNumberTestCase(
        id="fractional_value_below_the_threshold_rounds_to_the_nearest_whole_number",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(1234.6, "1,235")],
    ),
    FormatLargeNumberTestCase(
        id="value_just_below_one_million_stays_comma_formatted",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(999999.0, "999,999")],
    ),
    FormatLargeNumberTestCase(
        id="value_exactly_one_million_is_abbreviated_to_millions",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(1000000.0, "1.000m")],
    ),
    FormatLargeNumberTestCase(
        id="value_just_above_one_million_is_abbreviated_to_millions",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(1175000.0, "1.175m")],
    ),
    FormatLargeNumberTestCase(
        id="whole_millions_value_still_shows_three_decimals",
        column_name=Pub.publication_filled_posts,
        column_alias=Pub.publication_filled_posts_formatted,
        expected_data=[(2000000.0, "2.000m")],
    ),
    FormatLargeNumberTestCase(
        id="honours_the_column_name_and_alias_arguments",
        column_name=Pub.assessment_filled_posts_long_term,
        column_alias=Pub.assessment_filled_posts_long_term_formatted,
        expected_data=[(3500000.0, "3.500m")],
    ),
]


# --- Ticket 2107 spike: diagnostic thresholds ---
_FP = DT.Metric.filled_posts
_NR = DT.WorkbookServiceType.non_residential
_CHN = DT.WorkbookServiceType.care_home_with_nursing
_CH = DT.WorkbookServiceType.care_homes
_MONTHLY = DT.IntervalType.monthly
_QUARTERLY = DT.IntervalType.quarterly


@dataclass
class DiagnosticExpectedDataTestCase:
    id: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


add_interval_type_test_cases = [
    DiagnosticExpectedDataTestCase(
        id="labels_step_monthly_when_previous_period_is_one_month_earlier",
        expected_data=[
            (_FP, _NR, date(2024, 4, 1), 100.0, None),
            (_FP, _NR, date(2024, 5, 1), 101.0, _MONTHLY),
            (_FP, _NR, date(2024, 6, 1), 102.0, _MONTHLY),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="labels_step_quarterly_when_previous_period_is_three_months_earlier",
        expected_data=[
            (_FP, _NR, date(2023, 10, 1), 100.0, None),
            (_FP, _NR, date(2024, 1, 1), 101.0, _QUARTERLY),
            (_FP, _NR, date(2024, 4, 1), 102.0, _QUARTERLY),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="leaves_first_period_in_series_unlabelled",
        expected_data=[
            (_FP, _NR, date(2024, 5, 1), 100.0, None),
            (_FP, _CHN, date(2024, 5, 1), 50.0, None),
            (_FP, _NR, date(2024, 6, 1), 101.0, _MONTHLY),
        ],
    ),
]

add_period_on_period_change_test_cases = [
    DiagnosticExpectedDataTestCase(
        id="calculates_pct_change_from_previous_period_within_series",
        expected_data=[
            (_FP, _NR, date(2024, 4, 1), 100.0, None),
            (_FP, _NR, date(2024, 5, 1), 110.0, 0.1),
            (_FP, _NR, date(2024, 6, 1), 99.0, -0.1),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="returns_null_for_first_period_in_series",
        expected_data=[
            (_FP, _NR, date(2024, 4, 1), 100.0, None),
            (_FP, _CHN, date(2024, 4, 1), 50.0, None),
            (_FP, _CHN, date(2024, 5, 1), 55.0, 0.1),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="returns_null_when_previous_value_is_zero",
        expected_data=[
            (_FP, _NR, date(2024, 4, 1), 0.0, None),
            (_FP, _NR, date(2024, 5, 1), 10.0, None),
            (_FP, _NR, date(2024, 6, 1), 15.0, 0.5),
        ],
    ),
]

add_change_since_march_test_cases = [
    DiagnosticExpectedDataTestCase(
        id="calculates_pct_change_from_march_of_same_financial_year",
        expected_data=[
            (_FP, _NR, date(2026, 3, 1), 100.0, 0.0),
            (_FP, _NR, date(2026, 4, 1), 110.0, 0.1),
            (_FP, _NR, date(2026, 5, 1), 105.0, 0.05),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="uses_april_as_baseline_when_march_not_retained",
        expected_data=[
            (_FP, _NR, date(2023, 4, 1), 200.0, 0.0),
            (_FP, _NR, date(2023, 7, 1), 210.0, 0.05),
            (_FP, _NR, date(2023, 10, 1), 220.0, 0.1),
            (_FP, _NR, date(2024, 1, 1), 190.0, -0.05),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="resets_baseline_each_financial_year",
        expected_data=[
            (_FP, _NR, date(2025, 3, 1), 100.0, 0.0),
            (_FP, _NR, date(2026, 2, 1), 120.0, 0.2),
            (_FP, _NR, date(2026, 3, 1), 150.0, 0.0),
            (_FP, _NR, date(2026, 4, 1), 165.0, 0.1),
        ],
    ),
    DiagnosticExpectedDataTestCase(
        id="returns_zero_for_baseline_period",
        expected_data=[
            (_FP, _NR, date(2026, 3, 1), 80.0, 0.0),
            (_FP, _CHN, date(2026, 3, 1), 50.0, 0.0),
        ],
    ),
]


@dataclass
class DiagnosticInputExpectedTestCase:
    id: str
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_SFC_LONG = DT.sfc_filled_posts_metric("long_term")
_CT_LONG = DT.ct_total_employed_metric("long_term")
_GAP_LONG = DT.sfc_minus_ct_metric("long_term")
_SFC_MEDIUM = DT.sfc_filled_posts_metric("medium_term")
_CT_MEDIUM = DT.ct_total_employed_metric("medium_term")
_GAP_MEDIUM = DT.sfc_minus_ct_metric("medium_term")

add_sfc_ct_gap_test_cases = [
    DiagnosticInputExpectedTestCase(
        id="subtracts_ct_step_change_from_sfc_step_change",
        input_data=[
            (_SFC_LONG, _NR, date(2024, 5, 1), _MONTHLY, 0.02),
            (_CT_LONG, _NR, date(2024, 5, 1), _MONTHLY, 0.005),
            (_FP, _NR, date(2024, 5, 1), _MONTHLY, 0.5),
        ],
        expected_data=[
            (_GAP_LONG, _NR, date(2024, 5, 1), _MONTHLY, 0.015),
        ],
    ),
    DiagnosticInputExpectedTestCase(
        id="calculates_gap_separately_per_window_and_service_type",
        input_data=[
            (_SFC_LONG, _CH, date(2024, 5, 1), _MONTHLY, 0.01),
            (_CT_LONG, _CH, date(2024, 5, 1), _MONTHLY, 0.03),
            (_SFC_MEDIUM, _CH, date(2024, 5, 1), _MONTHLY, 0.04),
            (_CT_MEDIUM, _CH, date(2024, 5, 1), _MONTHLY, 0.01),
            (_SFC_LONG, _NR, date(2024, 5, 1), _MONTHLY, 0.0),
            (_CT_LONG, _NR, date(2024, 5, 1), _MONTHLY, -0.02),
        ],
        expected_data=[
            (_GAP_LONG, _CH, date(2024, 5, 1), _MONTHLY, -0.02),
            (_GAP_LONG, _NR, date(2024, 5, 1), _MONTHLY, 0.02),
            (_GAP_MEDIUM, _CH, date(2024, 5, 1), _MONTHLY, 0.03),
        ],
    ),
    DiagnosticInputExpectedTestCase(
        id="returns_null_gap_when_either_side_is_null",
        input_data=[
            (_SFC_LONG, _NR, date(2024, 5, 1), _MONTHLY, None),
            (_CT_LONG, _NR, date(2024, 5, 1), _MONTHLY, 0.01),
            (_SFC_LONG, _NR, date(2024, 6, 1), _MONTHLY, 0.01),
            (_CT_LONG, _NR, date(2024, 6, 1), _MONTHLY, None),
        ],
        expected_data=[
            (_GAP_LONG, _NR, date(2024, 5, 1), _MONTHLY, None),
            (_GAP_LONG, _NR, date(2024, 6, 1), _MONTHLY, None),
        ],
    ),
]


@dataclass
class ToMetricLongFormatTestCase:
    id: str
    input_data: list[Any]
    metrics: list[str]
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_LC = DT.Metric.location_count
_CHWO = DT.WorkbookServiceType.care_home_without_nursing
_ALL = DT.WorkbookServiceType.all_cqc_locations
_ASSESSMENT_METRICS = [
    metric_name(term)
    for term in DT.ASSESSMENT_TERMS
    for metric_name in (DT.sfc_filled_posts_metric, DT.ct_total_employed_metric)
]
_SFC_SHORT = DT.sfc_filled_posts_metric("short_term")
_CT_SHORT = DT.ct_total_employed_metric("short_term")

# Input rows: import date, job role, region, service type, publication filled
# posts, publication location count, then assessment filled posts and CT
# employed for the long, medium and short terms.
to_metric_long_format_test_cases = [
    ToMetricLongFormatTestCase(
        id="keeps_only_england_all_job_roles_rows",
        input_data=[
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.non_residential,
                100.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2026, 5, 1),
                "Care Worker",
                "England",
                PrimaryServiceType.non_residential,
                200.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "London",
                PrimaryServiceType.non_residential,
                300.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
        ],
        metrics=[_FP],
        expected_data=[(_FP, _NR, date(2026, 5, 1), 100.0)],
    ),
    ToMetricLongFormatTestCase(
        id="maps_publication_groups_to_the_five_workbook_service_types",
        input_data=[
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                "All CQC locations",
                500.0,
                50,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                "All CQC care homes",
                400.0,
                40,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.care_home_with_nursing,
                300.0,
                30,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.care_home_only,
                100.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.non_residential,
                100.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
        ],
        metrics=[_FP],
        expected_data=[
            (_FP, _ALL, date(2026, 5, 1), 500.0),
            (_FP, _CH, date(2026, 5, 1), 400.0),
            (_FP, _CHN, date(2026, 5, 1), 300.0),
            (_FP, _CHWO, date(2026, 5, 1), 100.0),
            (_FP, _NR, date(2026, 5, 1), 100.0),
        ],
    ),
    ToMetricLongFormatTestCase(
        id="outputs_filled_posts_and_location_count_metrics",
        input_data=[
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.non_residential,
                100.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
        ],
        metrics=[_FP, _LC],
        expected_data=[
            (_FP, _NR, date(2026, 5, 1), 100.0),
            (_LC, _NR, date(2026, 5, 1), 10.0),
        ],
    ),
    ToMetricLongFormatTestCase(
        id="outputs_sfc_and_ct_series_per_assessment_window_for_care_homes_and_non_res",
        input_data=[
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.non_residential,
                100.0,
                10,
                10.0,
                11.0,
                20.0,
                21.0,
                30.0,
                31.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                "All CQC care homes",
                100.0,
                10,
                40.0,
                41.0,
                50.0,
                51.0,
                60.0,
                61.0,
            ),
            (
                date(2026, 5, 1),
                "All job roles",
                "England",
                PrimaryServiceType.care_home_with_nursing,
                100.0,
                10,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ),
            (
                date(2025, 1, 1),
                "All job roles",
                "England",
                PrimaryServiceType.non_residential,
                100.0,
                10,
                70.0,
                71.0,
                80.0,
                81.0,
                90.0,
                91.0,
            ),
        ],
        metrics=_ASSESSMENT_METRICS,
        expected_data=[
            (_SFC_LONG, _NR, date(2026, 5, 1), 10.0),
            (_CT_LONG, _NR, date(2026, 5, 1), 11.0),
            (_SFC_MEDIUM, _NR, date(2026, 5, 1), 20.0),
            (_CT_MEDIUM, _NR, date(2026, 5, 1), 21.0),
            (_SFC_SHORT, _NR, date(2026, 5, 1), 30.0),
            (_CT_SHORT, _NR, date(2026, 5, 1), 31.0),
            (_SFC_LONG, _CH, date(2026, 5, 1), 40.0),
            (_CT_LONG, _CH, date(2026, 5, 1), 41.0),
            (_SFC_MEDIUM, _CH, date(2026, 5, 1), 50.0),
            (_CT_MEDIUM, _CH, date(2026, 5, 1), 51.0),
            (_SFC_SHORT, _CH, date(2026, 5, 1), 60.0),
            (_CT_SHORT, _CH, date(2026, 5, 1), 61.0),
            (_SFC_LONG, _NR, date(2025, 1, 1), 70.0),
            (_CT_LONG, _NR, date(2025, 1, 1), 71.0),
        ],
    ),
]


def _monthly_periods(start: date, count: int) -> list[date]:
    periods = []
    year, month = start.year, start.month
    for _ in range(count):
        periods.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return periods


def _series_rows(
    metric: str,
    service_type: str,
    periods: list[date],
    interval_type: str,
    values: list[float | None],
    expected: list[tuple[bool | None, bool]],
) -> list[tuple]:
    # Rows of (metric, service_type, period, interval_type, value, breached,
    # insufficient_history).
    return [
        (metric, service_type, period, interval_type, value, *flags)
        for period, value, flags in zip(periods, values, expected)
    ]


@dataclass
class HistoryMethodTestCase:
    id: str
    k: float
    value_col: str
    rows: list[tuple]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_SIX_INSUFFICIENT = [(None, True)] * 6
_ALTERNATING_SMALL = [0.01, -0.01] * 3
_ALTERNATING_LARGE = [0.05, -0.05] * 3
_APR_2024 = date(2024, 4, 1)
_POP = DT.Cols.period_on_period_change
_CSM = DT.Cols.change_since_march
_QUARTERS_TO_JAN_2024 = [
    date(2022, 10, 1),
    date(2023, 1, 1),
    date(2023, 4, 1),
    date(2023, 7, 1),
    date(2023, 10, 1),
    date(2024, 1, 1),
]

flag_mean_std_test_cases = [
    HistoryMethodTestCase(
        id="flags_change_outside_mean_plus_or_minus_k_std_of_prior_steps",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            _ALTERNATING_SMALL + [0.05],
            _SIX_INSUFFICIENT + [(True, False)],
        ),
    ),
    HistoryMethodTestCase(
        id="does_not_flag_change_inside_band",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            _ALTERNATING_SMALL + [0.02],
            _SIX_INSUFFICIENT + [(False, False)],
        ),
    ),
    HistoryMethodTestCase(
        id="uses_only_prior_periods_to_set_band",
        k=2,
        value_col=_POP,
        rows=list(
            reversed(
                _series_rows(
                    _FP,
                    _NR,
                    _monthly_periods(_APR_2024, 8),
                    _MONTHLY,
                    _ALTERNATING_SMALL + [0.05, 5.0],
                    _SIX_INSUFFICIENT + [(True, False), (True, False)],
                )
            )
        ),
    ),
    HistoryMethodTestCase(
        id="returns_insufficient_history_when_fewer_than_six_prior_steps",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 6),
            _MONTHLY,
            _ALTERNATING_SMALL[:5] + [5.0],
            _SIX_INSUFFICIENT,
        ),
    ),
    HistoryMethodTestCase(
        id="sets_bands_separately_per_interval_type",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _QUARTERS_TO_JAN_2024,
            _QUARTERLY,
            _ALTERNATING_LARGE,
            _SIX_INSUFFICIENT,
        )
        + _series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            _ALTERNATING_SMALL + [0.04],
            _SIX_INSUFFICIENT + [(True, False)],
        ),
    ),
    HistoryMethodTestCase(
        id="sets_bands_separately_per_metric_and_service_type",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            _ALTERNATING_SMALL + [0.04],
            _SIX_INSUFFICIENT + [(True, False)],
        )
        + _series_rows(
            _FP,
            _CHN,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            _ALTERNATING_LARGE + [0.04],
            _SIX_INSUFFICIENT + [(False, False)],
        )
        + _series_rows(
            _LC,
            _NR,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            _ALTERNATING_LARGE + [0.04],
            _SIX_INSUFFICIENT + [(False, False)],
        ),
    ),
    HistoryMethodTestCase(
        id="compares_since_march_values_only_against_same_month_in_prior_years",
        k=2,
        value_col=_CSM,
        rows=_series_rows(
            _FP,
            _NR,
            [date(year, 5, 1) for year in range(2019, 2026)],
            _MONTHLY,
            _ALTERNATING_SMALL + [0.04],
            _SIX_INSUFFICIENT + [(True, False)],
        )
        + _series_rows(
            _FP,
            _NR,
            [date(year, 8, 1) for year in range(2019, 2025)],
            _MONTHLY,
            [0.2, -0.2] * 3,
            _SIX_INSUFFICIENT,
        ),
    ),
]

flag_median_mad_test_cases = [
    HistoryMethodTestCase(
        id="flags_change_outside_median_plus_or_minus_k_mad_of_prior_steps",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 8),
            _MONTHLY,
            [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.04],
            _SIX_INSUFFICIENT + [(True, False), (False, False)],
        ),
    ),
    HistoryMethodTestCase(
        id="single_prior_outlier_does_not_widen_band",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 7),
            _MONTHLY,
            [0.01, 0.02, 0.03, 0.04, 0.05, 1.0, 0.08],
            _SIX_INSUFFICIENT + [(True, False)],
        ),
    ),
    HistoryMethodTestCase(
        id="returns_insufficient_history_when_fewer_than_six_prior_steps",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 6),
            _MONTHLY,
            [0.01, 0.02, 0.03, 0.04, 0.05, 5.0],
            _SIX_INSUFFICIENT,
        ),
    ),
    HistoryMethodTestCase(
        id="flags_any_deviation_when_prior_steps_are_all_identical",
        k=2,
        value_col=_POP,
        rows=_series_rows(
            _FP,
            _NR,
            _monthly_periods(_APR_2024, 8),
            _MONTHLY,
            [0.01] * 6 + [0.011, 0.01],
            _SIX_INSUFFICIENT + [(True, False), (False, False)],
        ),
    ),
]


@dataclass
class BandMethodTestCase:
    id: str
    rows: list[tuple]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


# Fixed band / cross-sectional rows: (metric, service_type, period,
# interval_type, value, lower, upper, breached). Monthly limit 0.02,
# quarterly 0.05.
_MAY_2026 = date(2026, 5, 1)
_JAN_2024 = date(2024, 1, 1)

flag_fixed_band_test_cases = [
    BandMethodTestCase(
        id="flags_monthly_change_outside_monthly_tolerance",
        rows=[
            (_FP, _NR, date(2024, 5, 1), _MONTHLY, 0.03, -0.02, 0.02, True),
            (_FP, _NR, date(2024, 6, 1), _MONTHLY, -0.03, -0.02, 0.02, True),
            (_FP, _NR, date(2024, 7, 1), _MONTHLY, 0.01, -0.02, 0.02, False),
        ],
    ),
    BandMethodTestCase(
        id="applies_quarterly_tolerance_to_quarterly_steps",
        rows=[
            (_FP, _NR, date(2023, 10, 1), _QUARTERLY, 0.03, -0.05, 0.05, False),
            (_FP, _NR, _JAN_2024, _QUARTERLY, 0.06, -0.05, 0.05, True),
        ],
    ),
    BandMethodTestCase(
        id="does_not_flag_change_exactly_on_tolerance",
        rows=[
            (_FP, _NR, date(2024, 5, 1), _MONTHLY, 0.02, -0.02, 0.02, False),
            (_FP, _NR, _JAN_2024, _QUARTERLY, -0.05, -0.05, 0.05, False),
        ],
    ),
]

flag_cross_sectional_test_cases = [
    BandMethodTestCase(
        id="flags_base_type_whose_gap_from_median_of_others_exceeds_limit",
        rows=[
            (_FP, _CHN, _MAY_2026, _MONTHLY, 0.002, -0.034, 0.006, False),
            (_FP, _CHWO, _MAY_2026, _MONTHLY, 0.002, -0.034, 0.006, False),
            (_FP, _NR, _MAY_2026, _MONTHLY, -0.03, -0.018, 0.022, True),
        ],
    ),
    BandMethodTestCase(
        id="does_not_flag_when_all_base_types_move_together",
        rows=[
            (_FP, _CHN, _JAN_2024, _QUARTERLY, -0.03, -0.08, 0.02, False),
            (_FP, _CHWO, _JAN_2024, _QUARTERLY, -0.03, -0.08, 0.02, False),
            (_FP, _NR, _JAN_2024, _QUARTERLY, -0.03, -0.08, 0.02, False),
        ],
    ),
    BandMethodTestCase(
        id="excludes_rollup_service_types_from_comparison",
        rows=[
            (_FP, _ALL, _MAY_2026, _MONTHLY, -0.5, None, None, None),
            (_FP, _CH, _MAY_2026, _MONTHLY, -0.5, None, None, None),
            (_FP, _CHN, _MAY_2026, _MONTHLY, 0.0, -0.02, 0.02, False),
            (_FP, _CHWO, _MAY_2026, _MONTHLY, 0.0, -0.02, 0.02, False),
            (_FP, _NR, _MAY_2026, _MONTHLY, 0.0, -0.02, 0.02, False),
        ],
    ),
    BandMethodTestCase(
        id="returns_null_when_other_base_types_missing_for_period",
        rows=[
            (_FP, _NR, _MAY_2026, _MONTHLY, 0.01, None, None, None),
            (_LC, _NR, _MAY_2026, _MONTHLY, 0.01, None, None, None),
            (_LC, _CHN, _MAY_2026, _MONTHLY, None, None, None, None),
        ],
    ),
]


# Rows: (warn setting breached, error setting breached, expected tier)
assign_tier_test_cases = [
    DiagnosticExpectedDataTestCase(
        id="returns_error_when_error_setting_breached",
        expected_data=[(True, True, DT.Tier.error)],
    ),
    DiagnosticExpectedDataTestCase(
        id="returns_warn_when_only_warn_setting_breached",
        expected_data=[(True, False, DT.Tier.warn)],
    ),
    DiagnosticExpectedDataTestCase(
        id="returns_none_when_neither_breached",
        expected_data=[(False, False, DT.Tier.none)],
    ),
    DiagnosticExpectedDataTestCase(
        id="passes_through_insufficient_history",
        expected_data=[(None, None, DT.Tier.insufficient_history)],
    ),
]


@dataclass
class SummariseBacktestTestCase:
    id: str
    filter_on_flags: list[tuple]
    filter_off_flags: list[tuple]
    expected_breaks: list[tuple]
    expected_data: list[tuple]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


# Flag rows: (method, setting, basis, metric, service_type, period, value,
# breached). Summary rows: (method, setting, basis, metric, service_type,
# evaluated_periods, false_alarms, expected_breaks, detected_breaks).
_KEY = ("median_mad", "k=2", _POP, _FP, _NR)
_BD214_BREAKS = [(_FP, _NR, date(2024, 4, 1)), (_FP, _NR, date(2026, 5, 1))]

summarise_backtest_test_cases = [
    SummariseBacktestTestCase(
        id="counts_flags_on_filter_on_data_as_false_alarms",
        filter_on_flags=[
            (*_KEY, date(2025, 1, 1), 0.1, True),
            (*_KEY, date(2025, 2, 1), 0.0, False),
            (*_KEY, date(2025, 3, 1), 0.1, True),
            (*_KEY, date(2025, 4, 1), 0.0, None),
        ],
        filter_off_flags=[(*_KEY, date(2025, 1, 1), 0.1, True)],
        expected_breaks=[],
        expected_data=[(*_KEY, 3, 2, 0, 0)],
    ),
    SummariseBacktestTestCase(
        id="marks_detected_when_expected_break_period_flagged_on_filter_off_data",
        filter_on_flags=[
            (*_KEY, date(2024, 4, 1), 0.0, False),
            (*_KEY, date(2026, 5, 1), 0.0, False),
        ],
        filter_off_flags=[
            (*_KEY, date(2024, 4, 1), -0.1, True),
            (*_KEY, date(2025, 1, 1), 0.1, True),
            (*_KEY, date(2026, 5, 1), 0.1, True),
        ],
        expected_breaks=_BD214_BREAKS,
        expected_data=[(*_KEY, 2, 0, 2, 2)],
    ),
    SummariseBacktestTestCase(
        id="reports_not_detected_when_break_period_not_flagged",
        filter_on_flags=[
            (*_KEY, date(2024, 4, 1), 0.0, False),
            (*_KEY, date(2026, 5, 1), 0.0, False),
        ],
        filter_off_flags=[
            (*_KEY, date(2024, 4, 1), -0.1, False),
            (*_KEY, date(2026, 5, 1), 0.1, None),
        ],
        expected_breaks=_BD214_BREAKS,
        expected_data=[(*_KEY, 2, 0, 2, 0)],
    ),
]
