import unittest
import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.models.interpolation as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH = "projects._04_direct_payment_recipients.fargate.utils.models.interpolation"


class TestDPRModelInterpolation(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        rows = [
            ("area_1", 2021, 0.3, 0.3),
            ("area_2", 2021, 0.4, 0.4),
            ("area_1", 2020, None, 0.3375),
            ("area_2", 2020, 0.35, 0.35),
            ("area_1", 2019, 0.375, 0.375),
            ("area_2", 2019, 0.2, 0.2),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
            DP.ESTIMATE_USING_INTERPOLATION: pl.Float64,
        }
        expected_lf = pl.LazyFrame(rows, schema=test_schema, orient="row")
        test_lf = expected_lf.drop(DP.ESTIMATE_USING_INTERPOLATION)
        returned_lf = job.model_interpolation(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestInterpolateValuesForAllDates(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        rows = [
            ("area_1", 2019, 0.5, 2019, 0.5),
            ("area_1", 2020, None, None, 0.5),
            ("area_1", 2021, 0.5, 2021, 0.5),
            ("area_2", 2019, 0.3, 2019, 0.3),
            ("area_2", 2020, None, None, 0.4),
            ("area_2", 2021, 0.5, 2021, 0.5),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
            DP.ESTIMATE_USING_INTERPOLATION: pl.Float64,
        }

        base_lf = pl.LazyFrame(rows, schema=test_schema, orient="row")
        test_lf = base_lf.drop(DP.ESTIMATE_USING_INTERPOLATION)
        expected_lf = base_lf.drop(
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
        )
        returned_lf = job.interpolate_values_for_all_dates(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestGetPreviousValueInColumn(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        rows = [
            ("area_1", 2019, 2019, 2019),
            ("area_1", 2020, None, 2019),
            ("area_1", 2021, 2021, 2021),
            ("area_2", 2019, 2019, 2019),
            ("area_2", 2020, None, 2019),
            ("area_2", 2021, 2021, 2021),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
            DP.PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
        }

        expected_lf = pl.LazyFrame(rows, test_schema, orient="row")
        test_lf = expected_lf.drop(
            DP.PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
        )
        returned_lf = job.get_previous_value_in_column(
            test_lf,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
            DP.PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestGetNextValueInColumn(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        rows = [
            ("area_1", 2019, 2019, 2019),
            ("area_1", 2020, None, 2021),
            ("area_1", 2021, 2021, 2021),
            ("area_2", 2019, 2019, 2019),
            ("area_2", 2020, None, 2021),
            ("area_2", 2021, 2021, 2021),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
            DP.NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
        }

        expected_lf = pl.LazyFrame(rows, test_schema, orient="row")
        test_lf = expected_lf.drop(
            DP.NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
        )
        returned_lf = job.get_next_value_in_new_column(
            test_lf,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
            DP.NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCalculatedInterpolatedValuesInNewColumn(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        rows = [
            ("area_1", 2019, 10, 10, 10, 2019, 2019, 10.0),
            ("area_1", 2020, 15, 10, 20, 2019, 2021, 15.0),
            ("area_1", 2021, 20, 20, 20, 2021, 2021, 20.0),
            ("area_2", 2019, 5, 5, 5, 2019, 2019, 5.0),
            ("area_2", 2020, 10, 5, 15, 2019, 2021, 10.0),
            ("area_2", 2021, 15, 15, 15, 2021, 2021, 15.0),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Int64,
            DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF: pl.Int64,
            DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF: pl.Int64,
            DP.PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
            DP.NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: pl.Int64,
            DP.ESTIMATE_USING_INTERPOLATION: pl.Float64,
        }

        base_lf = pl.LazyFrame(rows, test_schema, orient="row")
        test_lf = base_lf.drop(DP.ESTIMATE_USING_INTERPOLATION)
        expected_lf = base_lf.select(
            DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATE_USING_INTERPOLATION
        )
        returned_lf = job.calculated_interpolated_values_in_new_column(
            test_lf,
            DP.ESTIMATE_USING_INTERPOLATION,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
