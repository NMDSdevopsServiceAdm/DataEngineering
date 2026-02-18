from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._02_clean.utils.forward_fill_latest_known_value as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ForwardFillLatestKnownValue as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ForwardFillLatestKnownValue as Schemas,
)
from tests.base_test import SparkBaseTest
from utils import utils

PATCH_PATH = (
    "projects._03_independent_cqc._02_clean.utils.forward_fill_latest_known_value"
)


class ReturnLastKnownValueTests(SparkBaseTest):
    def setUp(self) -> None: ...

    def test_last_known_returns_latest_non_null_value_per_location(
        self,
    ):
        test_df = self.spark.createDataFrame(
            data=Data.last_known_latest_per_location_rows,
            schema=Schemas.input_return_last_known_value_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_last_known_latest_per_location_rows,
            schema=Schemas.expected_return_last_known_value_locations_schema,
        )
        returned_df = job.return_last_known_value(test_df, "col_to_repeat")
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_last_known_ignores_null_values_when_identifying_last_known(
        self,
    ):
        test_df = self.spark.createDataFrame(
            data=Data.last_known_ignores_null_rows,
            schema=Schemas.input_return_last_known_value_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_last_known_ignores_null_rows,
            schema=Schemas.expected_return_last_known_value_locations_schema,
        )
        returned_df = job.return_last_known_value(test_df, "col_to_repeat")
        self.assertEqual(returned_df.collect(), expected_df.collect())


class ForwardFillTests(SparkBaseTest):
    def setUp(self) -> None: ...

    def test_forward_fill_populates_null_values_within_days_to_repeat_range(
        self,
    ):
        test_df = self.spark.createDataFrame(
            data=Data.forward_fill_within_days_rows,
            schema=Schemas.input_forward_fill_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_forward_fill_within_days_rows,
            schema=Schemas.expected_forward_fill_locations_schema,
        )
        returned_df = job.forward_fill(test_df, "col_to_repeat", days_to_repeat=2)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_forward_fill_does_not_populate_null_values_beyond_days_to_repeat_range(
        self,
    ):
        test_df = self.spark.createDataFrame(
            data=Data.forward_fill_beyond_days_rows,
            schema=Schemas.input_forward_fill_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_forward_fill_beyond_days_rows,
            schema=Schemas.expected_forward_fill_locations_schema,
        )
        returned_df = job.forward_fill(test_df, "col_to_repeat", days_to_repeat=2)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_forward_fill_does_not_populate_null_values_before_last_known_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            data=Data.forward_fill_before_last_known_rows,
            schema=Schemas.input_forward_fill_locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            data=Data.expected_forward_fill_before_last_known_rows,
            schema=Schemas.expected_forward_fill_locations_schema,
        )
        returned_df = job.forward_fill(test_df, "col_to_repeat", days_to_repeat=2)
        self.assertEqual(returned_df.collect(), expected_df.collect())


class ForwardFillLatestKnownValueCallTests(SparkBaseTest):
    def setUp(self):
        self.df = self.spark.createDataFrame(
            Data.forward_fill_latest_known_value_rows,
            Schemas.forward_fill_latest_known_value_locations_schema,
        )

    @patch(f"{PATCH_PATH}.forward_fill")
    @patch(f"{PATCH_PATH}.return_last_known_value")
    def test_forward_fill_latest_known_value_calls_all_subfunctions(
        self, return_last_known_value_mock: Mock, forward_fill_mock: Mock
    ):
        return_last_known_value_mock.return_value = self.df
        forward_fill_mock.return_value = self.df
        job.forward_fill_latest_known_value(self.df, "col_to_repeat", days_to_repeat=2)

        return_last_known_value_mock.assert_called_once()

        forward_fill_mock.assert_called_once()
