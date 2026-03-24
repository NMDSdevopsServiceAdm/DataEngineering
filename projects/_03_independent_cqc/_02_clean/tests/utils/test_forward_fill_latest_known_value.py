from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_clean.utils.forward_fill_latest_known_value as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ForwardFillLatestKnownValue as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ForwardFillLatestKnownValue as Schemas,
)
from tests.base_test import SparkBaseTest

PATCH_PATH = (
    "projects._03_independent_cqc._02_clean.utils.forward_fill_latest_known_value"
)


class ForwardFillLatestKnownValueCallTests(SparkBaseTest):
    @patch(f"{PATCH_PATH}.forward_fill")
    @patch(f"{PATCH_PATH}.add_size_based_forward_fill_days")
    @patch(f"{PATCH_PATH}.return_last_known_value")
    def test_forward_fill_latest_known_value_calls_all_subfunctions(
        self,
        return_last_known_value_mock: Mock,
        add_size_based_forward_fill_days_mock: Mock,
        forward_fill_mock: Mock,
    ):
        input_df = Mock(name="input_df")
        add_size_based_forward_fill_days_mock.return_value = Mock(name="last_known_df")
        job.forward_fill_latest_known_value(input_df, Schemas.col_to_forward_fill)

        return_last_known_value_mock.assert_called_once()
        add_size_based_forward_fill_days_mock.assert_called_once()
        forward_fill_mock.assert_called_once()

    def test_dict_of_size_based_forward_fill_days_values_are_correct(self):
        self.assertEqual(
            job.SIZE_BASED_FORWARD_FILL_DAYS,
            Data.expected_size_based_forward_fill_days_dict,
        )


class AddSizeBasedForwardFillDaysTests(SparkBaseTest):
    def test_adds_days_to_forward_fill_column_based_on_location_size(self):
        test_df = self.spark.createDataFrame(
            data=Data.size_based_forward_fill_days_rows,
            schema=Schemas.size_based_forward_fill_days_schema,
        )
        returned_df = job.add_size_based_forward_fill_days(
            test_df,
            Schemas.col_to_forward_fill,
            Data.TEST_SIZE_BASED_FORWARD_FILL_DAYS,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_size_based_forward_fill_days_rows,
            schema=Schemas.expected_size_based_forward_fill_days_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class ReturnLastKnownValueTests(SparkBaseTest):
    def test_last_known_returns_latest_non_null_value_per_location(self):
        test_df = self.spark.createDataFrame(
            data=Data.last_known_latest_per_location_rows,
            schema=Schemas.input_return_last_known_value_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_last_known_latest_per_location_rows,
            schema=Schemas.expected_return_last_known_value_locations_schema,
        )
        returned_df = job.return_last_known_value(test_df, Schemas.col_to_forward_fill)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_last_known_ignores_null_values_when_identifying_last_known(self):
        test_df = self.spark.createDataFrame(
            data=Data.last_known_ignores_null_rows,
            schema=Schemas.input_return_last_known_value_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_last_known_ignores_null_rows,
            schema=Schemas.expected_return_last_known_value_locations_schema,
        )
        returned_df = job.return_last_known_value(test_df, Schemas.col_to_forward_fill)
        self.assertEqual(returned_df.collect(), expected_df.collect())


class ForwardFillTests(SparkBaseTest):
    def test_populates_null_values_within_days_to_forward_fill_range(self):
        test_df = self.spark.createDataFrame(
            data=Data.forward_fill_within_days_rows,
            schema=Schemas.forward_fill_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_forward_fill_within_days_rows,
            schema=Schemas.forward_fill_schema,
        )
        returned_df = job.forward_fill(test_df, Schemas.col_to_forward_fill)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_does_not_populate_null_values_beyond_days_to_forward_fill_range(self):
        test_df = self.spark.createDataFrame(
            data=Data.forward_fill_beyond_days_rows,
            schema=Schemas.forward_fill_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_forward_fill_beyond_days_rows,
            schema=Schemas.forward_fill_schema,
        )
        returned_df = job.forward_fill(test_df, Schemas.col_to_forward_fill)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_does_not_populate_null_values_before_last_known_value(self):
        input_df = self.spark.createDataFrame(
            data=Data.forward_fill_before_last_known_rows,
            schema=Schemas.forward_fill_schema,
        )
        returned_df = job.forward_fill(input_df, Schemas.col_to_forward_fill)
        self.assertEqual(returned_df.collect(), input_df.collect())
