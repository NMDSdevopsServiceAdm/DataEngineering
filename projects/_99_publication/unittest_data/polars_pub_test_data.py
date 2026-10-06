from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)


@dataclass
class ReducedDataFilterTestCase:
    id: str
    today: date | None
    fy_start_month: int
    lookback_fy_years: int
    quarter_months: tuple[int, ...]
    input_data: list[date]
    expected: list[bool]
    cutoff_date: date | None = None

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


reduced_data_filter_test_cases = [
    ReducedDataFilterTestCase(
        id="default args",
        today=date(2024, 6, 15),
        fy_start_month=4,
        lookback_fy_years=2,
        quarter_months=(1, 4, 7, 10),
        input_data=[
            date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
            date(2021, 5, 1), # before monthly_start, non-quarter -> excluded
            date(2022, 3, 31), # before monthly_start and quarterly rule does not match -> excluded
            date(2022, 4, 1), # at boundary (monthly_start) -> included
            date(2023, 6, 1), # within range -> included
        ],
        expected=[True, False, False, True, True],
    ),
    ReducedDataFilterTestCase(
        id="non_default_args",
        today=date(2024, 6, 15),
        fy_start_month=1,
        lookback_fy_years=1,
        quarter_months=(3, 6, 9, 12),
        input_data=[
            date(2022, 1, 1), # before monthly_start, non-quarter -> excluded
            date(2022, 2, 1), # before monthly_start, non-quarter -> excluded
            date(2022, 12, 1), # before monthly_start but quarterly rule matches -> included
            date(2023, 3, 1), # before monthly_start and quarterly rule matches -> included
            date(2024, 6, 1), # within range -> included
        ],
        expected=[False, False, True, True, True],
    ),
    ReducedDataFilterTestCase(
        id="today_defaults_to_current_date",
        today=None,
        fy_start_month=4,
        lookback_fy_years=2,
        quarter_months=(1, 4, 7, 10),
        input_data=[
            date.today(),  # should be included as it's the current date
            date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
            date(2021, 5, 1), # before monthly_start, non-quarter -> excluded
        ],
        expected=[True, True, False],
    ),
    ReducedDataFilterTestCase(
        id="cutoff_date_excludes_rows_before_given_date",
        today=date(2024, 6, 15),
        fy_start_month=4,
        lookback_fy_years=2,
        quarter_months=(1, 4, 7, 10),
        cutoff_date=date(2020, 4, 1),
        input_data=[
            date(2019, 4, 1), # quarterly rule matches but before cutoff_date -> excluded
            date(2020, 3, 31), # immediately before cutoff date -> excluded
            date(2020, 4, 1), # at cutoff_date, quarterly rule matches -> included
            date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
            date(2022, 4, 1), # at boundary (monthly_start) -> included
            date(2023, 6, 1), # within range -> included
        ],
        expected=[False, False, True, True, True, True],
    ),
]  # fmt: skip


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
                PublishedJobGroupLabels.all_job_roles,
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
                PublishedMainService.all_locations,
                *_all_terms_metrics(25.0, 2, 11.0),
            ),
            (
                date(2025, 4, 1),
                "Registered nurse",
                "London",
                PublishedMainService.all_care_homes,
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
                PublishedRegion.england,
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


@dataclass
class CalcPercChangeAgainstPeriodsAgoTestCase:
    id: str
    column_name: str
    periods_back: int
    group_columns: list[str]
    column_alias: str
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


DOWNLOAD_TABLE_GROUP_COLUMNS = [
    IndCQC.current_region,
    IndCQC.primary_service_type,
]
_LONDON_CARE_HOME_WITH_NURSING = ("London", _CARE_HOME_WITH_NURSING)
_SOUTH_WEST_NON_RES = ("South West", "Non-residential service")

calc_perc_change_against_periods_ago_test_cases = [
    CalcPercChangeAgainstPeriodsAgoTestCase(
        id="change_is_measured_against_periods_back_rows_earlier",
        column_name=Pub.publication_filled_posts,
        periods_back=2,
        group_columns=DOWNLOAD_TABLE_GROUP_COLUMNS,
        column_alias=Pub.annual_percentage_change,
        expected_data=[
            (date(2025, 1, 1), *_LONDON_CARE_HOME_WITH_NURSING, 100.0, None, None),
            (date(2025, 2, 1), *_LONDON_CARE_HOME_WITH_NURSING, 150.0, None, None),
            (date(2025, 3, 1), *_LONDON_CARE_HOME_WITH_NURSING, 200.0, None, 1.0),
        ],
    ),
    CalcPercChangeAgainstPeriodsAgoTestCase(
        id="null_for_every_row_before_periods_back_have_elapsed",
        column_name=Pub.publication_filled_posts,
        periods_back=3,
        group_columns=DOWNLOAD_TABLE_GROUP_COLUMNS,
        column_alias=Pub.annual_percentage_change,
        expected_data=[
            (date(2025, 1, 1), *_LONDON_CARE_HOME_WITH_NURSING, 100.0, None, None),
            (date(2025, 2, 1), *_LONDON_CARE_HOME_WITH_NURSING, 150.0, None, None),
        ],
    ),
    CalcPercChangeAgainstPeriodsAgoTestCase(
        id="null_when_the_earlier_value_is_zero",
        column_name=Pub.publication_filled_posts,
        periods_back=1,
        group_columns=DOWNLOAD_TABLE_GROUP_COLUMNS,
        column_alias=Pub.monthly_percentage_change,
        expected_data=[
            (date(2025, 1, 1), *_LONDON_CARE_HOME_WITH_NURSING, 0.0, None, None),
            (date(2025, 2, 1), *_LONDON_CARE_HOME_WITH_NURSING, 50.0, None, None),
        ],
    ),
    CalcPercChangeAgainstPeriodsAgoTestCase(
        id="a_value_dropping_to_zero_is_a_full_decrease_not_a_null",
        column_name=Pub.publication_filled_posts,
        periods_back=1,
        group_columns=DOWNLOAD_TABLE_GROUP_COLUMNS,
        column_alias=Pub.monthly_percentage_change,
        expected_data=[
            (date(2025, 1, 1), *_LONDON_CARE_HOME_WITH_NURSING, 100.0, None, None),
            (date(2025, 2, 1), *_LONDON_CARE_HOME_WITH_NURSING, 0.0, None, -1.0),
        ],
    ),
    CalcPercChangeAgainstPeriodsAgoTestCase(
        # Rows are deliberately out of date order within each group, so this
        # only passes if the lag genuinely orders by import date rather than
        # relying on physical row order.
        id="groups_do_not_affect_each_other",
        column_name=Pub.publication_filled_posts,
        periods_back=1,
        group_columns=DOWNLOAD_TABLE_GROUP_COLUMNS,
        column_alias=Pub.monthly_percentage_change,
        expected_data=[
            (date(2025, 2, 1), *_LONDON_CARE_HOME_WITH_NURSING, 200.0, None, 1.0),
            (date(2025, 2, 1), *_SOUTH_WEST_NON_RES, 25.0, None, -0.5),
            (date(2025, 1, 1), *_LONDON_CARE_HOME_WITH_NURSING, 100.0, None, None),
            (date(2025, 1, 1), *_SOUTH_WEST_NON_RES, 50.0, None, None),
        ],
    ),
    CalcPercChangeAgainstPeriodsAgoTestCase(
        id="honours_the_column_name_periods_back_group_columns_and_alias_arguments",
        column_name=Pub.publication_locationid_count,
        periods_back=1,
        group_columns=DOWNLOAD_TABLE_GROUP_COLUMNS,
        column_alias=Pub.annual_percentage_change,
        expected_data=[
            (date(2025, 1, 1), *_LONDON_CARE_HOME_WITH_NURSING, None, 40, None),
            (date(2025, 2, 1), *_LONDON_CARE_HOME_WITH_NURSING, None, 60, 0.5),
        ],
    ),
]


@dataclass
class BuildT0EstimatesDownloadTableTestCase:
    id: str
    today: date
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


@dataclass
class BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase:
    id: str
    today: date
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self) -> pytest.param:
        return pytest.param(self, id=self.id)


_ALL_CQC_LOCATIONS = PublishedMainService.all_locations
_ALL_CQC_CARE_HOMES = PublishedMainService.all_care_homes
_ALL_JOB_ROLES = PublishedJobGroupLabels.all_job_roles
_PUBLISHED_CARE_HOME_WITH_NURSING = PublishedMainService.care_home_with_nursing
_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING = (
    "London",
    _PUBLISHED_CARE_HOME_WITH_NURSING,
)

# fy_start_month defaults to 4 (April), so this puts the financial year start
# (and so the annual/monthly boundary) at 2026-04-01.
_TODAY = date(2026, 10, 6)

build_t0_estimates_download_table_test_cases = [
    BuildT0EstimatesDownloadTableTestCase(
        # One row per year historically (at the financial-year-start month,
        # labelled as the year before - e.g. 2026-04-01 -> "Mar-26"), full
        # monthly detail from the financial year start onwards. The
        # 2025-07-01 row is a historical month other than the
        # financial-year-start month, so it's dropped.
        id="keeps_one_row_per_year_historically_and_full_monthly_for_the_current_financial_year",
        today=_TODAY,
        input_data=[
            (date(2024, 4, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2025, 4, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 110.0, 11),
            (date(2025, 7, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 999.0, 99),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 120.0, 12),
            (date(2026, 5, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 121.0, 12),
            (date(2026, 6, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 122.0, 13),
        ],
        expected_data=[
            (date(2024, 4, 1), "Mar-24", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2025, 4, 1), "Mar-25", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 110.0, 11),
            (date(2026, 4, 1), "Mar-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 120.0, 12),
            (date(2026, 5, 1), "Apr-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 121.0, 12),
            (date(2026, 6, 1), "May-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 122.0, 13),
        ],
    ),
    BuildT0EstimatesDownloadTableTestCase(
        # The job-role rows are dropped - only the "All job roles" rollup
        # feeds the download, which has no job-role breakdown.
        id="filters_out_individual_job_role_rows",
        today=_TODAY,
        input_data=[
            (date(2026, 4, 1), "Registered nurse", "London", _CARE_HOME_WITH_NURSING, 40.0, 10),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 100.0, 10),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 100.0, 10),
        ],
    ),
    BuildT0EstimatesDownloadTableTestCase(
        # Region/service-type rollups are not job-role rows, so they pass
        # through the "All job roles" filter like any other region/service.
        id="keeps_region_and_service_type_rollup_rows",
        today=_TODAY,
        input_data=[
            (date(2026, 4, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "London", _ALL_CQC_CARE_HOMES, 150.0, 15),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "London", _ALL_CQC_LOCATIONS, 200.0, 20),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "England", _ALL_CQC_LOCATIONS, 500.0, 50),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", "England", _ALL_CQC_LOCATIONS, 500.0, 50),
            (date(2026, 4, 1), "Mar-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 4, 1), "Mar-26", "London", _ALL_CQC_CARE_HOMES, 150.0, 15),
            (date(2026, 4, 1), "Mar-26", "London", _ALL_CQC_LOCATIONS, 200.0, 20),
        ],
    ),
    BuildT0EstimatesDownloadTableTestCase(
        # Deliberately out of (period, region, main_service) order on input,
        # and mixing an annual row with a current-financial-year monthly row
        # - only passes if the output is genuinely sorted, not just passed
        # through in input order.
        id="sorts_by_period_then_region_then_main_service",
        today=_TODAY,
        input_data=[
            (date(2026, 5, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 110.0, 11),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "South West", _CARE_HOME_WITH_NURSING, 90.0, 9),
            (date(2026, 4, 1), _ALL_JOB_ROLES, "London", _CARE_HOME_WITH_NURSING, 100.0, 10),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 4, 1), "Mar-26", "South West", _PUBLISHED_CARE_HOME_WITH_NURSING, 90.0, 9),
            (date(2026, 5, 1), "Apr-26", "London", _PUBLISHED_CARE_HOME_WITH_NURSING, 110.0, 11),
        ],
    ),
]  # fmt: skip

build_t1_filled_posts_perc_change_download_table_test_cases = [
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        # Every row here is an annual (financial-year-start-month) row, so
        # annual_percentage_change is populated once a prior year exists and
        # monthly_percentage_change stays null throughout - there's no
        # monthly granularity to compare across annual rows.
        id="annual_percentage_change_is_populated_for_annual_rows_monthly_change_stays_null",
        today=_TODAY,
        input_data=[
            (date(2024, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2025, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 150.0, 12),
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 200.0, 15),
        ],
        expected_data=[
            (date(2024, 4, 1), "Mar-24", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2025, 4, 1), "Mar-25", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, 0.5, None),
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, 0.3333333, None),
        ],
    ),
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        # The financial-year-start row itself is annual (both null, no prior
        # year in this data) - every row after it is monthly, so
        # monthly_percentage_change is populated and annual_percentage_change
        # stays null.
        id="monthly_percentage_change_is_populated_for_current_financial_year_rows_annual_change_stays_null",
        today=_TODAY,
        input_data=[
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 200.0, 15),
            (date(2026, 5, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 220.0, 16),
            (date(2026, 6, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 198.0, 14),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2026, 5, 1), "Apr-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, 0.1),
            (date(2026, 6, 1), "May-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, -0.1),
        ],
    ),
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        # The first monthly row (2026-05-01) has no monthly row before it -
        # its monthly change compares against the last annual row
        # (2026-04-01), one month earlier, rather than being null.
        id="first_monthly_row_compares_against_the_prior_annual_row",
        today=_TODAY,
        input_data=[
            (date(2025, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 200.0, 15),
            (date(2026, 5, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 220.0, 16),
        ],
        expected_data=[
            (date(2025, 4, 1), "Mar-25", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, 1.0, None),
            (date(2026, 5, 1), "Apr-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, 0.1),
        ],
    ),
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        id="filters_out_individual_job_role_rows_before_computing_change",
        today=_TODAY,
        input_data=[
            (date(2026, 4, 1), "Registered nurse", *_LONDON_CARE_HOME_WITH_NURSING, 40.0, 5),
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 5, 1), "Registered nurse", *_LONDON_CARE_HOME_WITH_NURSING, 999.0, 99),
            (date(2026, 5, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 150.0, 12),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2026, 5, 1), "Apr-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, 0.5),
        ],
    ),
]  # fmt: skip

build_t2_location_count_perc_change_download_table_test_cases = [
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        id="annual_percentage_change_is_populated_for_annual_rows_monthly_change_stays_null",
        today=_TODAY,
        input_data=[
            (date(2024, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2025, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 12),
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 15),
        ],
        expected_data=[
            (date(2024, 4, 1), "Mar-24", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2025, 4, 1), "Mar-25", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, 0.2, None),
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, 0.25, None),
        ],
    ),
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        id="monthly_percentage_change_is_populated_for_current_financial_year_rows_annual_change_stays_null",
        today=_TODAY,
        input_data=[
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 15),
            (date(2026, 5, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 16),
            (date(2026, 6, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 14),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2026, 5, 1), "Apr-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, 0.0666667),
            (date(2026, 6, 1), "May-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, -0.125),
        ],
    ),
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        id="first_monthly_row_compares_against_the_prior_annual_row",
        today=_TODAY,
        input_data=[
            (date(2025, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 15),
            (date(2026, 5, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 16),
        ],
        expected_data=[
            (date(2025, 4, 1), "Mar-25", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, 0.5, None),
            (date(2026, 5, 1), "Apr-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, 0.0666667),
        ],
    ),
    BuildFilledPostsOrLocationCountPercChangeDownloadTableTestCase(
        id="filters_out_individual_job_role_rows_before_computing_change",
        today=_TODAY,
        input_data=[
            (date(2026, 4, 1), "Registered nurse", *_LONDON_CARE_HOME_WITH_NURSING, 40.0, 5),
            (date(2026, 4, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 100.0, 10),
            (date(2026, 5, 1), "Registered nurse", *_LONDON_CARE_HOME_WITH_NURSING, 999.0, 99),
            (date(2026, 5, 1), _ALL_JOB_ROLES, *_LONDON_CARE_HOME_WITH_NURSING, 150.0, 12),
        ],
        expected_data=[
            (date(2026, 4, 1), "Mar-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, None),
            (date(2026, 5, 1), "Apr-26", *_LONDON_PUBLISHED_CARE_HOME_WITH_NURSING, None, 0.2),
        ],
    ),
]  # fmt: skip
