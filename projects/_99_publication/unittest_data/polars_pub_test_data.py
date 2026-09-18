from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

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
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_CARE_HOME_WITH_NURSING = PrimaryServiceType.care_home_with_nursing
_NON_RESIDENTIAL = PrimaryServiceType.non_residential

add_rows_for_publication_groups_test_cases = [
    AddRowsForPublicationGroupsTestCase(
        # Location "1-001" has two job roles, so a naive sum of the two rows'
        # publication_locationid_count would give 2 for "All job roles" - it
        # must stay 1, since it is still only one location.
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
        expected_data=[
            # real rows, unchanged
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(20.0, 1, 8.0),
            ),
            # "All job roles" - filled posts sum to 30, but it is still the
            # same single location, so locationid_count stays 1.
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
            # Only one real service type is present, so both service type
            # rollups mirror the three London rows above under a new label.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC locations",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "London",
                "All CQC locations",
                *_all_terms_metrics(20.0, 1, 8.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                "All CQC locations",
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(20.0, 1, 8.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
            # Only one real region is present, so England mirrors every row
            # above - this is the "England ends up with all the previous
            # aggregation rows" property.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(20.0, 1, 8.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                "All CQC locations",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "England",
                "All CQC locations",
                *_all_terms_metrics(20.0, 1, 8.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                "All CQC locations",
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Care worker",
                "England",
                "All CQC care homes",
                *_all_terms_metrics(20.0, 1, 8.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                "All CQC care homes",
                *_all_terms_metrics(30.0, 1, 13.0),
            ),
        ],
    ),
    AddRowsForPublicationGroupsTestCase(
        # Two locations, one care home and one non-residential: "All CQC care
        # homes" must exclude the non-residential location while "All CQC
        # locations" includes it.
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
        expected_data=[
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _NON_RESIDENTIAL,
                *_all_terms_metrics(15.0, 1, 6.0),
            ),
            # Only one job role is present, so "All job roles" mirrors both
            # rows above under a new label.
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                _NON_RESIDENTIAL,
                *_all_terms_metrics(15.0, 1, 6.0),
            ),
            # "All CQC locations" sums both service types - two locations.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC locations",
                *_all_terms_metrics(25.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                "All CQC locations",
                *_all_terms_metrics(25.0, 2, 11.0),
            ),
            # "All CQC care homes" excludes the non-residential location.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            # England mirrors every row above - only one real region present.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                _NON_RESIDENTIAL,
                *_all_terms_metrics(15.0, 1, 6.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                _NON_RESIDENTIAL,
                *_all_terms_metrics(15.0, 1, 6.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                "All CQC locations",
                *_all_terms_metrics(25.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                "All CQC locations",
                *_all_terms_metrics(25.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
        ],
    ),
    AddRowsForPublicationGroupsTestCase(
        # Two locations in different real regions: England must sum across
        # both, once each - not double-count a region by also summing in a
        # rollup row derived from it (e.g. "All CQC locations") as if it
        # were a second, separate addend.
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
        expected_data=[
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "South West",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(12.0, 1, 6.0),
            ),
            # Only one job role is present in each region, so "All job roles"
            # mirrors both rows above under a new label.
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "South West",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(12.0, 1, 6.0),
            ),
            # Only one real service type is present (a care home type), so
            # both service type rollups mirror the four rows above.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC locations",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "South West",
                "All CQC locations",
                *_all_terms_metrics(12.0, 1, 6.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                "All CQC locations",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "South West",
                "All CQC locations",
                *_all_terms_metrics(12.0, 1, 6.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "South West",
                "All CQC care homes",
                *_all_terms_metrics(12.0, 1, 6.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "London",
                "All CQC care homes",
                *_all_terms_metrics(10.0, 1, 5.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "South West",
                "All CQC care homes",
                *_all_terms_metrics(12.0, 1, 6.0),
            ),
            # England sums London (10/1/5) + South West (12/1/6) exactly once
            # each = 22/2/11, for every job role/service type combination
            # present above, including the rollups themselves.
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(22.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                _CARE_HOME_WITH_NURSING,
                *_all_terms_metrics(22.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                "All CQC locations",
                *_all_terms_metrics(22.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                "All CQC locations",
                *_all_terms_metrics(22.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "England",
                "All CQC care homes",
                *_all_terms_metrics(22.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "All job roles",
                "England",
                "All CQC care homes",
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
