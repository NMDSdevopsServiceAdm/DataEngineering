from dataclasses import dataclass
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.estimate_service_users_employing_staff as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class EstimateServiceUsersEmployingStaffTestCase:
    id: str
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.input_data, self.expected_data, id=self.id)


estimated_service_users_employing_staff_test_cases = [
    EstimateServiceUsersEmployingStaffTestCase(
        id="known_proportions_in_all_years",
        input_data=[
            ("area_1", 2020, 10.0, 0.5, None),
            ("area_1", 2021, 10.0, 0.5, None),
        ], # fmt: skip
        expected_data=[
            ("area_1", 2020, 10.0, 0.5, None, 0.5, 2020, 2021, None, 0.5, 0.5, "proportion_su_only_employing_staff", 0.5, 5.0),
            ("area_1", 2021, 10.0, 0.5, None, 0.5, 2020, 2021, None, 0.5, 0.5, "proportion_su_only_employing_staff", 0.5, 5.0),
        ] # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="null_proportion_in_earliest_year",
        input_data=[
            ("area_1", 2020, 10.0, None, None),
            ("area_1", 2021, 10.0, 0.5, None),
            ("area_2", 2020, 10.0, 0.3, None),
            ("area_2", 2021, 10.0, 0.5, None),
        ], # fmt: skip
        expected_data=[
            ("area_1", 2020, 10.0, None, None, 0.3, 2021, 2021, 0.3, 0.3, 0.3, "estimate_using_extrapolation_ratio", 0.3, 3.0),
            ("area_1", 2021, 10.0, 0.5, None, 0.5, 2021, 2021, None, 0.5, 0.5, "proportion_su_only_employing_staff", 0.4, 4.0),
            ("area_2", 2020, 10.0, 0.3, None, 0.3, 2020, 2021, None, 0.3, 0.3, "proportion_su_only_employing_staff", 0.3, 3.0),
            ("area_2", 2021, 10.0, 0.5, None, 0.5, 2020, 2021, None, 0.5, 0.5, "proportion_su_only_employing_staff", 0.4, 4.0),
        ] # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="null_proportion_between_known_years",
        input_data=[
            ("area_1", 2020, 10.0, 0.5, None),
            ("area_1", 2021, 10.0, None, None),
            ("area_1", 2022, 10.0, 0.3, None),
        ], # fmt: skip
        expected_data=[
            ("area_1", 2020, 10.0, 0.5, None, 0.5, 2020, 2022, None, 0.5, 0.5, "proportion_su_only_employing_staff", 0.5, 5.0),
            ("area_1", 2021, 10.0, None, None, None, 2020, 2022, None, 0.4, 0.4, "estimate_using_interpolation", 0.45, 4.5),
            ("area_1", 2022, 10.0, 0.3, None, 0.3, 2020, 2022, None, 0.3, 0.3, "proportion_su_only_employing_staff", 0.4, 4.0),
        ] # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="only_historic_data_is_known",
        input_data=[
            ("area_1", 2020, 10.0, None, 0.5),
            ("area_1", 2021, 10.0, None, 0.4),
        ], # fmt: skip
        expected_data=[
            ("area_1", 2020, 10.0, None, 0.5, 0.5, None, None, None, 0.5, 0.5, "estimate_using_mean", 0.5, 5.0),
            ("area_1", 2021, 10.0, None, 0.4, 0.4, None, None, None, 0.4, 0.4, "estimate_using_mean", 0.45, 4.5),
        ] # fmt: skip
    ),
]


class TestEstimateServiceUsersEmployingStaff:
    @pytest.mark.parametrize(
        ("input_data", "expected_data"),
        [
            case.as_pytest_param()
            for case in estimated_service_users_employing_staff_test_cases
        ],
    )
    def test_function_returns_expected_values(self, input_data, expected_data):
        input_lf = pl.LazyFrame(
            input_data,
            schema={
                DP.LA_AREA: pl.String,
                DP.YEAR_AS_INTEGER: pl.Int64,
                DP.SERVICE_USER_DPRS_DURING_YEAR: pl.Float32,
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
                DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: pl.Float32,
            },
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            expected_data,
            schema={
                DP.LA_AREA: pl.String,
                DP.YEAR_AS_INTEGER: pl.Int64,
                DP.SERVICE_USER_DPRS_DURING_YEAR: pl.Float32,
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
                DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: pl.Float32,
                DP.ESTIMATE_USING_MEAN: pl.Float32,
                DP.FIRST_YEAR_WITH_DATA: pl.Int32,
                DP.LAST_YEAR_WITH_DATA: pl.Int32,
                DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: pl.Float32,
                DP.ESTIMATE_USING_INTERPOLATION: pl.Float32,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: pl.String,
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: pl.Float32,
            },
            orient="row",
        )
        returned_lf = job.calculate_estimated_service_users_employing_staff(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False
        )
