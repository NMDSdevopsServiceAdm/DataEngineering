import unittest
import polars as pl
import polars.testing as pl_testing
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ForwardFillLatestKnownValue as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ForwardFillLatestKnownValue as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value"


class ReturnLastKnownValueTests(unittest.TestCase):
    def test_last_known_returns_latest_non_null_value_per_location(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.last_known_latest_per_location_rows,
            schema=Schemas.input_return_last_known_value_locations_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_last_known_latest_per_location_rows,
            schema=Schemas.expected_return_last_known_value_locations_schema,
            orient="row",
        )
        returned_lf = job.return_last_known_value(test_lf, "col_to_forward_fill")
        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), expected_lf)

    def test_last_known_ignores_null_values_when_identifying_last_known(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.last_known_ignores_null_rows,
            schema=Schemas.input_return_last_known_value_locations_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_last_known_ignores_null_rows,
            schema=Schemas.expected_return_last_known_value_locations_schema,
            orient="row",
        )
        returned_lf = job.return_last_known_value(test_lf, "col_to_forward_fill")
        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), expected_lf)


class ForwardFillTests(unittest.TestCase):
    def test_forward_fill_populates_null_values_within_days_to_repeat_range(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.forward_fill_within_days_rows,
            schema=Schemas.forward_fill_locations_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_forward_fill_within_days_rows,
            schema=Schemas.forward_fill_locations_schema,
            orient="row",
        )
        returned_lf = job.forward_fill(test_lf, "col_to_forward_fill")
        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), expected_lf)

    def test_forward_fill_does_not_populate_null_values_beyond_days_to_repeat_range(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.forward_fill_beyond_days_rows,
            schema=Schemas.forward_fill_locations_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_forward_fill_beyond_days_rows,
            schema=Schemas.forward_fill_locations_schema,
            orient="row",
        )
        returned_lf = job.forward_fill(test_lf, "col_to_forward_fill")
        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), expected_lf)

    def test_forward_fill_does_not_populate_null_values_before_last_known_value(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.forward_fill_before_last_known_rows,
            schema=Schemas.forward_fill_locations_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_forward_fill_before_last_known_rows,
            schema=Schemas.forward_fill_locations_schema,
            orient="row",
        )
        returned_lf = job.forward_fill(test_lf, "col_to_forward_fill")
        pl_testing.assert_frame_equal(returned_lf.sort(IndCQC.location_id), expected_lf)


class AddSizeBasedForwardFillDaysTests(unittest.TestCase):
    def test_adds_days_to_forward_fill_column_based_on_location_size(self):
        test_lf = pl.LazyFrame(
            data=Data.size_based_forward_fill_days_rows,
            schema=Schemas.size_based_forward_fill_days_schema,
            orient="row",
        )
        returned_lf = job.add_size_based_forward_fill_days(
            test_lf,
            Schemas.col_to_forward_fill,
            Data.TEST_SIZE_BASED_FORWARD_FILL_DAYS,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_size_based_forward_fill_days_rows,
            schema=Schemas.expected_size_based_forward_fill_days_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class ForwardFillLatestKnownValueCallTests(unittest.TestCase):
    def setUp(self):
        self.lf = pl.LazyFrame(
            Data.forward_fill_latest_known_value_rows,
            Schemas.forward_fill_latest_known_value_locations_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.forward_fill")
    @patch(f"{PATCH_PATH}.add_size_based_forward_fill_days")
    @patch(f"{PATCH_PATH}.return_last_known_value")
    def test_forward_fill_latest_known_value_calls_all_subfunctions(
        self,
        return_last_known_value_mock: Mock,
        add_size_based_forward_fill_days_mock: Mock,
        forward_fill_mock: Mock,
    ):
        return_last_known_value_mock.return_value = self.lf
        add_size_based_forward_fill_days_mock.return_value = self.lf
        forward_fill_mock.return_value = self.lf
        job.forward_fill_latest_known_value(self.lf, "col_to_forward_fill")

        return_last_known_value_mock.assert_called_once()
        add_size_based_forward_fill_days_mock.assert_called_once()
        forward_fill_mock.assert_called_once()

    def test_dict_of_size_based_forward_fill_days_values_are_correct(self):
        self.assertEqual(
            job.SIZE_BASED_FORWARD_FILL_DAYS,
            Data.expected_size_based_forward_fill_days_dict,
        )
