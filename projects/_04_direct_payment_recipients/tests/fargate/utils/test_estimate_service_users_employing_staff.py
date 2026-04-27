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
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


estimated_service_users_employing_staff_test_cases = [
    EstimateServiceUsersEmployingStaffTestCase(
        id="Test_1",
        data=[
            ("area_1", 2021, 0.6, 0.5),
            ("area_1", 2020, 0.5, 0.4),
            ("area_1", 2019, 0.4, 0.3),
            ("area_1", 2018, 0.3, 0.2),
            ("area_1", 2017, 0.2, 0.15),
            ("area_1", 2016, 0.1, 0.1),
        ],
    ),
]


class TestEstimateServiceUsersEmployingStaff:
    @pytest.mark.parametrize(
        "test_data",
        [
            case.as_pytest_param()
            for case in estimated_service_users_employing_staff_test_cases
        ],
    )
    def test_function_returns_expected_values(self, test_data):
        expected_lf = pl.LazyFrame(
            test_data,
            schema={
                DP.LA_AREA: pl.String,
                DP.YEAR_AS_INTEGER: pl.Int64,
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
            },
            orient="row",
        )
        test_lf = expected_lf.drop(DP.ESTIMATE_USING_MEAN)
        returned_lf = job.calculate_estimated_service_users_employing_staff(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
