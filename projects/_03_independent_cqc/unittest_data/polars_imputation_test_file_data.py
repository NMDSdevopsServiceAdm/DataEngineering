from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from utils.column_values.categorical_column_values import CareHome


@dataclass
class ModelInterpolationTestCase:
    id: str
    data: list[Any]
    method: str = None
    max_days_between_submissions: int = None


@dataclass
class InterpolationData:
    interpolation_test_cases = [
        ModelInterpolationTestCase(
            id="trend_interpolation_single_gap",
            data=[
                ("1-001", date(2023, 3, 1), 40.0, None, None),
                ("1-001", date(2023, 6, 1), None, 46.0, 34.185792),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, None),
            ],
            method="trend",
        ),
        ModelInterpolationTestCase(
            id="trend_interpolation_not_possible_with_single_point",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 2, 1), 10.0, None, None),
                ("1-001", date(2023, 3, 1), None, None, None),
            ],
            method="trend",
        ),
        ModelInterpolationTestCase(
            id="straight_interpolation_single_gap",
            data=[
                ("1-001", date(2023, 3, 1), 40.0, None, None),
                ("1-001", date(2023, 4, 1), None, 42.0, 37.035519),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, None),
            ],
            method="straight",
        ),
        ModelInterpolationTestCase(
            id="straight_interpolation_not_possible_with_single_point",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 2, 1), 10.0, None, None),
                ("1-001", date(2023, 3, 1), None, None, None),
            ],
            method="straight",
        ),
    ]  # fmt: skip

    calculate_residual_test_cases = [
        ModelInterpolationTestCase(
            id="extrapolation_forwards_is_known_rows",
            data=[
                ("1-001", date(2023, 5, 1), None, 44.0, -47.0),
                ("1-001", date(2023, 4, 1), None, 42.0, -47.0),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, -47.0),
                ("1-001", date(2024, 4, 1), None, 5.1, 9.8),
                ("1-001", date(2024, 5, 1), 15.0, 5.2, 9.8),
            ],
        ),
        ModelInterpolationTestCase(
            id="extrapolation_forwards_is_none_rows",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 3, 1), 40.0, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="returns_none_date_after_final_non_null_submission_rows",
            data=[
                ("1-001", date(2024, 5, 1), 15.0, 5.2, 9.8),
                ("1-001", date(2024, 6, 1), None, 15.3, None),
            ],
        ),
    ]  # fmt: skip
    calculate_days_between_submissions_test_cases = [
        ModelInterpolationTestCase(
            id="one_location_data",
            data=[
                ("1-001", date(2024, 2, 1), None, None, None),
                ("1-001", date(2024, 3, 1), 5.0, None, None),
                ("1-001", date(2024, 4, 1), None, 122, 0.25),
                ("1-001", date(2024, 5, 1), None, 122, 0.5),
                ("1-001", date(2024, 6, 1), None, 122, 0.75),
                ("1-001", date(2024, 7, 1), 15.0, None, None),
                ("1-001", date(2023, 8, 1), None, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="multiple_locations_single_submission_each",
            data=[
                ("1-001", date(2024, 1, 1), 5.0, None, None),
                ("1-001", date(2024, 2, 1), None, None, None),
                ("1-002", date(2024, 1, 5), 3.0, None, None),
                ("1-002", date(2024, 2, 5), None, None, None),
                ("1-003", date(2024, 3, 1), 8.0, None, None),
                ("1-003", date(2024, 4, 1), None, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="two_locations_interleaved",
            data=[
                ("1-001", date(2024, 1, 1), 10.0, None, None),
                ("1-002", date(2024, 1, 2), 10.0, None, None),
                ("1-001", date(2024, 2, 1), None, 60, 0.51),
                ("1-002", date(2024, 3, 2), None, 90, 0.66),
                ("1-001", date(2024, 3, 1), 7.0, None, None),
                ("1-002", date(2024, 4, 1), 7.0, None, None),
            ],
        ),
    ]  # fmt: skip
    calculate_interpolated_values_test_cases = [
        ModelInterpolationTestCase(
            id="test_function_returns_expected_values_when_within_max_days",
            data=[
                ("1-001", date(2025, 1, 3), 20.0, None, None, None, None, None),
                ("1-001", date(2025, 1, 4), None, 20.0, 10.0, 4, 0.25, 22.5),
                ("1-001", date(2025, 1, 5), None, 20.0, 10.0, 4, 0.5, 25.0),
                ("1-001", date(2025, 1, 6), None, 20.0, 10.0, 4, 0.75, 27.5),
                ("1-001", date(2025, 1, 7), 30.0, 20.0, 10.0, None, None, None),
                ("1-001", date(2025, 1, 8), None, None, None, None, None, None),
            ],
            max_days_between_submissions=4,
        ),
        ModelInterpolationTestCase(
            id="test_function_returns_expected_values_when_outside_max_days",
            data=[
                ("1-001", date(2025, 1, 3), 20.0, None, None, None, None, None),
                ("1-001", date(2025, 1, 4), None, 20.0, 10.0, 3, 0.25, None),
                ("1-001", date(2025, 1, 5), None, 20.0, 10.0, 3, 0.5, None),
                ("1-001", date(2025, 1, 6), None, 20.0, 10.0, 3, 0.75, None),
                ("1-001", date(2025, 1, 7), 30.0, 20.0, 10.0, None, None, None),
                ("1-001", date(2025, 1, 8), None, None, None, None, None, None),
            ],
            max_days_between_submissions=2,
        ),
    ]  # fmt: skip


@dataclass
class ExtrapolationTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


@dataclass
class ModelExtrapolation:
    extrapolation_when_nominal_test_cases = [
        ExtrapolationTestCase(
            id="when_one_later_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 20.0, 100.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, -60.0, -60.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_later_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_earlier_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_earlier_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_intermediate_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 15.0, 10.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 25.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 30.0, 35.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_only_one_observed_value_exists",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 20.0, 20.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_model_extrapolation_only_applies_after_last_submission",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, None),
                ("1-001", date(2026, 3, 1), 50.0, 40.0, 30.0, None),
                ("1-001", date(2026, 4, 1), None, 50.0, 60.0, 60.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_gaps_exist_across_full_time_series",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 20.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-001", date(2026, 4, 1), 20.0, 80.0, 70.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-001", date(2026, 5, 1), None, 100.0, 40.0, 40.0), # Forwards extrapolation from previously known value (20.0)
            ],
        ),
        ExtrapolationTestCase(
            id="when_more_than_one_location_needs_extrapolating",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
                ("1-002", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 0.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_no_data_points_are_available",
            data=[
                ("1-001", date(2026, 3, 1), None, None, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_dates_are_not_sorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
            ],
)
    ] # fmt: skip
    extrapolation_when_ratio_test_cases = [
        ExtrapolationTestCase(
            id="when_one_later_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 20.0, 100.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 4.0, 4.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_later_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_earlier_data_point_is_missing",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_earlier_data_points_are_missing",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_intermediate_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 15.0, 10.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 30.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 30.0, 45.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_only_one_observed_value_exists",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 15.0, 15.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_model_extrapolation_only_applies_after_last_submission",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, None),
                ("1-001", date(2026, 3, 1), 50.0, 40.0, 20.0, None),
                ("1-001", date(2026, 4, 1), None, 20.0, 25.0, 25.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_gaps_exist_across_full_time_series",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-002", date(2026, 3, 1), None, 30.0, 15.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-002", date(2026, 4, 1), 20.0, 80.0, 40.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-002", date(2026, 5, 1), None, 100.0, 25.0, 25.0), # Forwards extrapolation from previously known value (20.0)
            ],
        ),
        ExtrapolationTestCase(
            id="when_more_than_one_location_needs_extrapolating",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_no_data_points_are_available",
            data=[
                ("1-003", date(2026, 3, 1), None, None, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_dates_are_not_sorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
            ],
        ),
    ] # fmt: skip
    expected_extrapolation_when_error_rows = [
        ("1-001", date(2026, 1, 1), None, 10.0, None, None),
    ]

    expected_extrapolation_aggregates_rows = [
        ("1-001", date(2026, 1, 1), None, 10.0, date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-001", date(2026, 2, 1), 10.0, 20.0, date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-001", date(2026, 3, 1), 20.0, 30.0, date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-002", date(2026, 1, 1), 15.0, 40.0, date(2026, 1, 1), date(2026, 1, 1), 15.0, 40.0),
        ("1-002", date(2026, 2, 1), None, 50.0, date(2026, 1, 1), date(2026, 1, 1), 15.0, 40.0),
        ("1-003", date(2026, 1, 1), None, None, None, None, None, None),
    ] # fmt: skip

    get_previous_value_test_cases = [
        ExtrapolationTestCase(
            id="when_values_are_sequential_no_nulls",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), 20.0, 10.0),
                ("1-001", date(2026, 3, 1), 30.0, 20.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_values_have_null_gap",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-001", date(2026, 3, 1), None, 10.0),
                ("1-001", date(2026, 4, 1), 40.0, 10.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_values_have_leading_nulls",
            data=[
                ("1-001", date(2026, 1, 1), None, None),
                ("1-001", date(2026, 2, 1), None, None),
                ("1-001", date(2026, 3, 1), 30.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_groups_are_present",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-002", date(2026, 1, 1), 5.0, None),
                ("1-002", date(2026, 2, 1), None, 5.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_input_is_unsorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 10.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_all_values_are_null",
            data=[
                ("1-001", date(2026, 1, 1), None, None),
                ("1-001", date(2026, 2, 1), None, None),
            ],
        ),
    ]


@dataclass
class ModelImputationTestCase:
    id: str
    expected_data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.expected_data, id=self.id)


@dataclass
class ModelImputation:
    column_with_null_values_name: str = "null_values"
    model_column_name: str = "trend_model"
    imputed_values_column_name: str = "imputed_values"

    expected_model_imputation_test_cases = [
        ModelImputationTestCase(
            id="when_values_will_be_extrapolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, 19.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 20.0, 2.0, 20.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, 21.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_be_interpolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 20.0, 1.0, 20.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 21.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 22.0, 3.0, 22.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_be_extrapolated_and_interpolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 20.0, 1.0, 20.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 21.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 22.0, 3.0, 22.0),
                ("1-001", date(2023, 4, 1), CareHome.not_care_home, None, 4.0, 23.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_not_be_imputed_because_all_present",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 30.0, 1.0, 30.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 31.0, 2.0, 31.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 32.0, 3.0, 32.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_not_be_imputed_because_all_null",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, None),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, None),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, None),
            ],
        ),
        ModelImputationTestCase(
            id="when_given_multiple_service_types_only_non_res_gets_imputed_due_to_function_argument",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, 19.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 20.0, 2.0, 20.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, 21.0),
                ("1-002", date(2023, 1, 1), CareHome.care_home, None, 1.0, None),
                ("1-002", date(2023, 2, 1), CareHome.care_home, 20.0, 2.0, None),
                ("1-002", date(2023, 3, 1), CareHome.care_home, None, 3.0, None),
            ],
        ),
        ModelImputationTestCase(
            id="when_location_changes_care_home_status_only_the_matching_period_is_imputed",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 50.0, 1.0, 50.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 51.0),
                ("1-001", date(2023, 3, 1), CareHome.care_home, 50.0, 3.0, None),
                ("1-001", date(2023, 4, 1), CareHome.care_home, None, 4.0, None),
            ],
        ),
    ] # fmt: skip
