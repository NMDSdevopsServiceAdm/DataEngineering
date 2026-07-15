from dataclasses import dataclass
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.estimate_service_users_employing_staff as job
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
                DP.LA_AREA: ["area_1", "area_1"],
                DP.YEAR_AS_INTEGER: [2020, 2021],
                DP.SERVICE_USER_DPRS_DURING_YEAR: [10.0, 10.0],
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.5],
                DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: [None, None],
                DP.ESTIMATE_USING_MEAN: [0.5, 0.5],
                DP.FIRST_YEAR_WITH_DATA: [2020, 2020],
                DP.LAST_YEAR_WITH_DATA: [2021, 2021],
                DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: [None, None],
                DP.ESTIMATE_USING_INTERPOLATION: [0.5, 0.5],
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.5],
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: ["proportion_su_only_employing_staff", "proportion_su_only_employing_staff"],
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.5],
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: [5.0, 5.0],
            }, # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="extrapolation_used_when_first_year_missing",
        expected_data={
                DP.LA_AREA: ["area_1", "area_1", "area_2", "area_2"],
                DP.YEAR_AS_INTEGER: [2020, 2021, 2020, 2021],
                DP.SERVICE_USER_DPRS_DURING_YEAR: [10.0, 10.0, 10.0, 10.0],
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [None, 0.5, 0.3, 0.5],
                DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: [None, None, None, None],
                DP.ESTIMATE_USING_MEAN: [0.3, 0.5, 0.3, 0.5],
                DP.FIRST_YEAR_WITH_DATA: [2021, 2021, 2020, 2020],
                DP.LAST_YEAR_WITH_DATA: [2021, 2021, 2021, 2021],
                DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: [0.3, None, None, None],
                DP.ESTIMATE_USING_INTERPOLATION: [0.3, 0.5, 0.3, 0.5],
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.3, 0.5, 0.3, 0.5],
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: [DP.ESTIMATE_USING_EXTRAPOLATION_RATIO, DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.3, 0.4, 0.3, 0.4],
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: [3.0, 4.0, 3.0, 4.0],
            }, # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="interpolation_used_to_populate_missing_value_between_known",
        expected_data={
            DP.LA_AREA: ["area_1", "area_1", "area_1"],
            DP.YEAR_AS_INTEGER: [2020, 2021, 2022],
            DP.SERVICE_USER_DPRS_DURING_YEAR: [10.0, 10.0, 10.0],
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, None, 0.3],
            DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: [None, None, None],
            DP.ESTIMATE_USING_MEAN: [0.5, None, 0.3],
            DP.FIRST_YEAR_WITH_DATA: [2020, 2020, 2020],
            DP.LAST_YEAR_WITH_DATA: [2022, 2022, 2022],
            DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: [None, None, None],
            DP.ESTIMATE_USING_INTERPOLATION: [0.5, 0.4, 0.3],
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.4, 0.3],
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: [DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, DP.ESTIMATE_USING_INTERPOLATION, DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.45, 0.4],
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: [5.0, 4.5, 4.0],
        } # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="mean_used_only_historic_estimate_available",
        expected_data={
            DP.LA_AREA: ["area_1", "area_1"],
            DP.YEAR_AS_INTEGER: [2020, 2021],
            DP.SERVICE_USER_DPRS_DURING_YEAR: [10.0, 10.0],
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [None, None],
            DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: [0.5, 0.4],
            DP.ESTIMATE_USING_MEAN: [0.5, 0.4],
            DP.FIRST_YEAR_WITH_DATA: [None, None],
            DP.LAST_YEAR_WITH_DATA: [None, None],
            DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: [None, None],
            DP.ESTIMATE_USING_INTERPOLATION: [0.5, 0.4],
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.4],
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: [DP.ESTIMATE_USING_MEAN, DP.ESTIMATE_USING_MEAN],
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [0.5, 0.45],
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: [5.0, 4.5],
        } # fmt: skip
    ),
    EstimateServiceUsersEmployingStaffTestCase(
        id="no_known_proportions_or_historic_proportions",
        expected_data={
            DP.LA_AREA: ["area_1", "area_1"],
            DP.YEAR_AS_INTEGER: [2020, 2021],
            DP.SERVICE_USER_DPRS_DURING_YEAR: [10.0, 10.0],
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [None, None],
            DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: [None, None],
            DP.ESTIMATE_USING_MEAN: [None, None],
            DP.FIRST_YEAR_WITH_DATA: [None, None],
            DP.LAST_YEAR_WITH_DATA: [None, None],
            DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: [None, None],
            DP.ESTIMATE_USING_INTERPOLATION: [None, None],
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [None, None],
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: [None, None],
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: [None, None],
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: [None, None],
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
    def test_function_returns_expected_values(self, expected_data):
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
        input_lf = expected_lf.drop(
            [
                DP.ESTIMATE_USING_MEAN,
                DP.FIRST_YEAR_WITH_DATA,
                DP.LAST_YEAR_WITH_DATA,
                DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
                DP.ESTIMATE_USING_INTERPOLATION,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE,
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
            ]
        )
        returned_lf = job.calculate_estimated_service_users_employing_staff(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False
        )
