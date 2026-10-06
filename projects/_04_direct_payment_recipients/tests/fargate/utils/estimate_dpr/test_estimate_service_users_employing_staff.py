from dataclasses import dataclass
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.estimate_dpr.estimate_service_users_employing_staff as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class EstimateServiceUsersEmployingStaffTestCase:
    id: str
    expected_data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.expected_data, id=self.id)


estimated_service_users_employing_staff_test_cases = [
    EstimateServiceUsersEmployingStaffTestCase(
        id="known_proportions_used_when_known",
        expected_data=
            {
                DP.la_area: ["area_1", "area_1"],
                DP.year_as_integer: [2020, 2021],
                DP.service_user_dprs_during_year: [10.0, 10.0],
                DP.proportion_employing_staff: [0.5, 0.5],
                DP.historic_service_users_employing_staff_estimate: [None, None],
                DP.estimate_using_mean: [0.5, 0.5],
                DP.first_year_with_data: [2020, 2020],
                DP.last_year_with_data: [2021, 2021],
                DP.estimate_using_extrapolation_ratio: [None, None],
                DP.estimate_using_interpolation: [0.5, 0.5],
                DP.imputed_proportion_employing_staff: [0.5, 0.5],
                DP.imputed_proportion_employing_staff_source: [DP.proportion_employing_staff, DP.proportion_employing_staff],
                DP.rolling_average_proportion_employing_staff: [0.5, 0.5],
                DP.estimated_service_users_employing_staff: [5.0, 5.0],
            }, # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="extrapolation_used_when_first_year_missing",
        expected_data={
                DP.la_area: ["area_1", "area_1", "area_2", "area_2"],
                DP.year_as_integer: [2020, 2021, 2020, 2021],
                DP.service_user_dprs_during_year: [10.0, 10.0, 10.0, 10.0],
                DP.proportion_employing_staff: [None, 0.5, 0.3, 0.5],
                DP.historic_service_users_employing_staff_estimate: [None, None, None, None],
                DP.estimate_using_mean: [0.3, 0.5, 0.3, 0.5],
                DP.first_year_with_data: [2021, 2021, 2020, 2020],
                DP.last_year_with_data: [2021, 2021, 2021, 2021],
                DP.estimate_using_extrapolation_ratio: [0.3, None, None, None],
                DP.estimate_using_interpolation: [0.3, 0.5, 0.3, 0.5],
                DP.imputed_proportion_employing_staff: [0.3, 0.5, 0.3, 0.5],
                DP.imputed_proportion_employing_staff_source: [DP.estimate_using_extrapolation_ratio, DP.proportion_employing_staff, DP.proportion_employing_staff, DP.proportion_employing_staff],
                DP.rolling_average_proportion_employing_staff: [0.3, 0.4, 0.3, 0.4],
                DP.estimated_service_users_employing_staff: [3.0, 4.0, 3.0, 4.0],
            }, # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="interpolation_used_to_populate_missing_value_between_known",
        expected_data={
            DP.la_area: ["area_1", "area_1", "area_1"],
            DP.year_as_integer: [2020, 2021, 2022],
            DP.service_user_dprs_during_year: [10.0, 10.0, 10.0],
            DP.proportion_employing_staff: [0.5, None, 0.3],
            DP.historic_service_users_employing_staff_estimate: [None, None, None],
            DP.estimate_using_mean: [0.5, None, 0.3],
            DP.first_year_with_data: [2020, 2020, 2020],
            DP.last_year_with_data: [2022, 2022, 2022],
            DP.estimate_using_extrapolation_ratio: [None, None, None],
            DP.estimate_using_interpolation: [0.5, 0.4, 0.3],
            DP.imputed_proportion_employing_staff: [0.5, 0.4, 0.3],
            DP.imputed_proportion_employing_staff_source: [DP.proportion_employing_staff, DP.estimate_using_interpolation, DP.proportion_employing_staff],
            DP.rolling_average_proportion_employing_staff: [0.5, 0.45, 0.4],
            DP.estimated_service_users_employing_staff: [5.0, 4.5, 4.0],
        } # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="mean_used_only_historic_estimate_available",
        expected_data={
            DP.la_area: ["area_1", "area_1"],
            DP.year_as_integer: [2020, 2021],
            DP.service_user_dprs_during_year: [10.0, 10.0],
            DP.proportion_employing_staff: [None, None],
            DP.historic_service_users_employing_staff_estimate: [0.5, 0.4],
            DP.estimate_using_mean: [0.5, 0.4],
            DP.first_year_with_data: [None, None],
            DP.last_year_with_data: [None, None],
            DP.estimate_using_extrapolation_ratio: [None, None],
            DP.estimate_using_interpolation: [0.5, 0.4],
            DP.imputed_proportion_employing_staff: [0.5, 0.4],
            DP.imputed_proportion_employing_staff_source: [DP.estimate_using_mean, DP.estimate_using_mean],
            DP.rolling_average_proportion_employing_staff: [0.5, 0.45],
            DP.estimated_service_users_employing_staff: [5.0, 4.5],
        } # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="no_known_proportions_or_historic_proportions",
        expected_data={
            DP.la_area: ["area_1", "area_1"],
            DP.year_as_integer: [2020, 2021],
            DP.service_user_dprs_during_year: [10.0, 10.0],
            DP.proportion_employing_staff: [None, None],
            DP.historic_service_users_employing_staff_estimate: [None, None],
            DP.estimate_using_mean: [None, None],
            DP.first_year_with_data: [None, None],
            DP.last_year_with_data: [None, None],
            DP.estimate_using_extrapolation_ratio: [None, None],
            DP.estimate_using_interpolation: [None, None],
            DP.imputed_proportion_employing_staff: [None, None],
            DP.imputed_proportion_employing_staff_source: [None, None],
            DP.rolling_average_proportion_employing_staff: [None, None],
            DP.estimated_service_users_employing_staff: [None, None],
        } # fmt: skip
    ),
]


class TestEstimateServiceUsersEmployingStaff:
    @pytest.mark.parametrize(
        "expected_data",
        [
            case.as_pytest_param()
            for case in estimated_service_users_employing_staff_test_cases
        ],
    )
    def test_calculate_estimated_service_users_employing_staff_returns_expected_values(
        self, expected_data
    ):
        expected_lf = pl.LazyFrame(
            expected_data,
            schema={
                DP.la_area: pl.String,
                DP.year_as_integer: pl.Int64,
                DP.service_user_dprs_during_year: pl.Float32,
                DP.proportion_employing_staff: pl.Float32,
                DP.historic_service_users_employing_staff_estimate: pl.Float32,
                DP.estimate_using_mean: pl.Float32,
                DP.first_year_with_data: pl.Int32,
                DP.last_year_with_data: pl.Int32,
                DP.estimate_using_extrapolation_ratio: pl.Float32,
                DP.estimate_using_interpolation: pl.Float32,
                DP.imputed_proportion_employing_staff: pl.Float32,
                DP.imputed_proportion_employing_staff_source: pl.String,
                DP.rolling_average_proportion_employing_staff: pl.Float32,
                DP.estimated_service_users_employing_staff: pl.Float32,
            },
            orient="row",
        )
        input_lf = expected_lf.drop(
            [
                DP.estimate_using_mean,
                DP.first_year_with_data,
                DP.last_year_with_data,
                DP.estimate_using_extrapolation_ratio,
                DP.estimate_using_interpolation,
                DP.imputed_proportion_employing_staff,
                DP.imputed_proportion_employing_staff_source,
                DP.rolling_average_proportion_employing_staff,
                DP.estimated_service_users_employing_staff,
            ]
        )
        returned_lf = job.calculate_estimated_service_users_employing_staff(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False
        )
