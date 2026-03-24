import warnings
from unittest.mock import Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelPrimaryServiceRateOfChange as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelPrimaryServiceRateOfChange as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change"
)


class MainTests(SparkBaseTest):
    def setUp(self) -> None:
        self.number_of_days: int = 4
        self.test_df = self.spark.createDataFrame(
            Data.primary_service_rate_of_change_rows,
            Schemas.primary_service_rate_of_change_schema,
        )
        self.returned_df = job.model_primary_service_rate_of_change(
            self.test_df,
            IndCqc.combined_ratio_and_filled_posts,
            self.number_of_days,
            IndCqc.single_period_rate_of_change,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rate_of_change_rows,
            Schemas.expected_primary_service_rate_of_change_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.unix_time,
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_primary_service_rate_of_change_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rate_of_change_model_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.single_period_rate_of_change],
                self.expected_data[i][IndCqc.single_period_rate_of_change],
                3,
                f"Returned row {i} does not match expected",
            )

    @patch(f"{PATCH_PATH}.calculate_new_column")
    @patch(f"{PATCH_PATH}.calculate_primary_service_rolling_sums")
    @patch(f"{PATCH_PATH}.clean_non_residential_rate_of_change")
    @patch(f"{PATCH_PATH}.add_previous_value_column")
    @patch(f"{PATCH_PATH}.interpolate_current_values")
    @patch(f"{PATCH_PATH}.remove_ineligible_locations")
    @patch(f"{PATCH_PATH}.calculate_windowed_column")
    def test_main_calls_functions(
        self,
        calculate_windowed_column_mock: Mock,
        remove_ineligible_locations_mock: Mock,
        interpolate_current_values_mock: Mock,
        add_previous_value_column_mock: Mock,
        clean_non_residential_rate_of_change_mock: Mock,
        calculate_primary_service_rolling_sums_mock: Mock,
        calculate_new_column_mock: Mock,
    ):
        calculate_new_column_mock.return_value = Mock(
            name="calculate_new_column_return_value"
        )

        job.model_primary_service_rate_of_change(
            self.test_df,
            IndCqc.combined_ratio_and_filled_posts,
            self.number_of_days,
            IndCqc.single_period_rate_of_change,
        )

        calculate_windowed_column_mock.assert_called_once()
        remove_ineligible_locations_mock.assert_called_once()
        interpolate_current_values_mock.assert_called_once()
        add_previous_value_column_mock.assert_called_once()
        clean_non_residential_rate_of_change_mock.assert_called_once()
        calculate_primary_service_rolling_sums_mock.assert_called_once()
        calculate_new_column_mock.assert_called_once()


class RemoveIneligibleLocationsTests(SparkBaseTest):
    def test_eligible_locations_are_not_filtered(self):
        input_df = self.spark.createDataFrame(
            Data.eligible_location_rows,
            Schemas.remove_ineligible_locations_schema,
        )
        returned_df = job.remove_ineligible_locations(input_df)

        returned_data = returned_df.sort(IndCqc.unix_time).collect()
        expected_data = input_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_location_removed_when_only_submitted_once(self):
        input_df = self.spark.createDataFrame(
            Data.remove_ineligible_locations_with_one_submission_rows,
            Schemas.remove_ineligible_locations_schema,
        )
        returned_df = job.remove_ineligible_locations(input_df)

        self.assertTrue(returned_df.isEmpty())

    def test_location_removed_when_has_multiple_care_home_statuses(self):
        input_df = self.spark.createDataFrame(
            Data.remove_ineligible_locations_with_multiple_statuses_rows,
            Schemas.remove_ineligible_locations_schema,
        )
        returned_df = job.remove_ineligible_locations(input_df)

        self.assertTrue(returned_df.isEmpty())


class InterpolateCurrentValuesTests(SparkBaseTest):
    def setUp(self) -> None:

        test_df = self.spark.createDataFrame(
            Data.interpolate_current_values_rows,
            Schemas.interpolate_current_values_schema,
        )
        self.returned_df = job.interpolate_current_values(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_current_values_rows,
            Schemas.expected_interpolate_current_values_schema,
        )
        self.returned_data = self.returned_df.sort(IndCqc.unix_time).collect()
        self.expected_data = self.expected_df.collect()

    def test_interpolate_current_values_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_interpolated_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.current_period_interpolated],
                self.expected_data[i][job.TempCol.current_period_interpolated],
                f"Returned row {i} does not match expected",
            )


class AddPreviousValueColumnTests(SparkBaseTest):
    def setUp(self) -> None:

        self.test_df = self.spark.createDataFrame(
            Data.add_previous_value_column_rows,
            Schemas.add_previous_value_column_schema,
        )
        self.returned_df = job.add_previous_value_column(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_previous_value_column_rows,
            Schemas.expected_add_previous_value_column_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_previous_interpolated_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.previous_period_interpolated],
                self.expected_data[i][job.TempCol.previous_period_interpolated],
                f"Returned row {i} does not match expected",
            )


class CalculatePrimaryServiceRollingSumsTests(SparkBaseTest):
    def setUp(self) -> None:

        self.number_of_days = 3
        self.current_col = job.TempCol.current_period_cleaned
        self.previous_col = job.TempCol.previous_period_cleaned

        self.test_df = self.spark.createDataFrame(
            Data.calculate_primary_service_rolling_sums_rows,
            Schemas.calculate_primary_service_rolling_sums_schema,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_primary_service_rolling_sums_rows,
            Schemas.expected_calculate_primary_service_rolling_sums_schema,
        )

        self.returned_df = job.calculate_primary_service_rolling_sums(
            self.test_df, self.number_of_days
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.unix_time,
        ).collect()

        self.expected_data = self.expected_df.collect()

    def test_unwanted_columns_are_dropped(self):
        self.assertNotIn(IndCqc.location_id, self.returned_df.columns)
        self.assertNotIn(self.current_col, self.returned_df.columns)
        self.assertNotIn(self.previous_col, self.returned_df.columns)

    def test_returned_column_names_match_expected(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_rolling_current_sum_is_correct(self):
        for i in range(len(self.expected_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_current_sum],
                self.expected_data[i][job.TempCol.rolling_current_sum],
                places=2,
                msg=f"Returned row {i} does not match expected",
            )

    def test_rolling_previous_sum_is_correct(self):
        for i in range(len(self.expected_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_previous_sum],
                self.expected_data[i][job.TempCol.rolling_previous_sum],
                places=2,
                msg=f"Returned row {i} does not match expected",
            )

    def _assert_rolling_sum_helper(self, input_rows, expected_rows):
        """Helper function to test rolling sum calculations with different input and expected data."""
        test_df = self.spark.createDataFrame(
            input_rows, Schemas.calculate_primary_service_rolling_sums_schema
        )
        expected_df = self.spark.createDataFrame(
            expected_rows,
            Schemas.expected_calculate_primary_service_rolling_sums_schema,
        )

        returned_df = job.calculate_primary_service_rolling_sums(
            test_df, self.number_of_days
        )

        returned_data = returned_df.sort(
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.unix_time,
        ).collect()

        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_rows_with_null_current_or_previous_are_filtered_out(self):
        self._assert_rolling_sum_helper(
            Data.rolling_sums_filtering_rows,
            Data.expected_rolling_sums_filtering_rows,
        )

    def test_simple_rolling_window(self):
        self._assert_rolling_sum_helper(
            Data.rolling_sums_simple_window_rows,
            Data.expected_rolling_sums_simple_window_rows,
        )

    def test_rolling_window_only_includes_rows_within_specified_number_of_days(self):
        self._assert_rolling_sum_helper(
            Data.rolling_sums_window_includes_values_within_range_rows,
            Data.expected_rolling_sums_window_includes_values_within_range_rows,
        )

    def test_window_partitions_correctly(self):
        self._assert_rolling_sum_helper(
            Data.rolling_sums_window_partitions_correctly_rows,
            Data.expected_rolling_sums_window_partitions_correctly_rows,
        )

    def test_deduplication_implemented_correctly(self):
        self._assert_rolling_sum_helper(
            Data.rolling_sums_deduplication_rows,
            Data.expected_rolling_sums_deduplication_rows,
        )
