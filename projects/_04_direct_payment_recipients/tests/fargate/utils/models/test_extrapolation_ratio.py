from dataclasses import dataclass
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.models.extrapolation_ratio as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class ModelExtrapolationTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


# Rows are (la_area, year, ...), used by the sort keys below to reorder the same data.
expected_rows = [
    ("area_1", 2018, None, 280.0, 2019, 2021, 0.35),
    ("area_1", 2019, 0.375, 300.0, 2019, 2021, None),
    ("area_1", 2020, None, 300.0, 2019, 2021, None),
    ("area_1", 2021, 0.3, 320.0, 2019, 2021, None),
    ("area_1", 2022, None, 340.0, 2019, 2021, 0.31875),
    ("area_2", 2018, None, 280.0, 2019, 2021, 0.186667),
    ("area_2", 2019, 0.2, 300.0, 2019, 2021, None),
    ("area_2", 2020, 0.35, 300.0, 2019, 2021, None),
    ("area_2", 2021, 0.4, 320.0, 2019, 2021, None),
    ("area_2", 2022, None, 340.0, 2019, 2021, 0.425),
]

model_extrapolation_test_cases = [
    ModelExtrapolationTestCase(id="returns_expected_values", data=expected_rows),
    ModelExtrapolationTestCase(
        id="returns_same_values_when_rows_reversed_within_la_area",
        data=sorted(expected_rows, key=lambda row: (row[0], -row[1])),
    ),
    ModelExtrapolationTestCase(
        id="returns_same_values_when_la_areas_interleaved",
        data=sorted(expected_rows, key=lambda row: (row[1], row[0])),
    ),
]


class TestModelExtrapolation:
    schema = {
        DP.LA_AREA: pl.String,
        DP.YEAR_AS_INTEGER: pl.Int32,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
        DP.ESTIMATE_USING_MEAN: pl.Float32,
        DP.FIRST_YEAR_WITH_DATA: pl.Int32,
        DP.LAST_YEAR_WITH_DATA: pl.Int32,
        DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: pl.Float32,
    }

    @pytest.mark.parametrize(
        "test_data", [c.as_pytest_param() for c in model_extrapolation_test_cases]
    )
    def test_function_returns_expected_values(self, test_data):
        expected_lf = pl.LazyFrame(test_data, self.schema, orient="row")
        test_lf = expected_lf.drop(
            DP.FIRST_YEAR_WITH_DATA,
            DP.LAST_YEAR_WITH_DATA,
            DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
        )

        returned_lf = job.model_extrapolation(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
