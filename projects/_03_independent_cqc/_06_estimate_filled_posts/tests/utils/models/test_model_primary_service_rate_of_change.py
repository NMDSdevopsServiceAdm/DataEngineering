import unittest
import warnings

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelPrimaryServiceRateOfChange as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelPrimaryServiceRateOfChange as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change"
)


class ModelPrimaryServiceRateOfChangeTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 3
        self.test_df = self.spark.createDataFrame(
            Data.primary_service_rate_of_change_rows,
            Schemas.primary_service_rate_of_change_schema,
        )
        self.returned_df = job.model_primary_service_rate_of_change(
            self.test_df,
            IndCqc.combined_ratio_and_filled_posts,
            number_of_days,
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


class RemoveIneligibleLocationsTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

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


class InterpolateCurrentValuesTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

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


class AddPreviousValueColumnTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

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


class CalculatePrimaryServiceRollingSumsTests(ModelPrimaryServiceRateOfChangeTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 2

        test_df = self.spark.createDataFrame(
            Data.calculate_primary_service_rolling_sums_rows,
            Schemas.calculate_primary_service_rolling_sums_schema,
        )
        self.returned_df = job.calculate_primary_service_rolling_sums(
            test_df, number_of_days
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_primary_service_rolling_sums_rows,
            Schemas.expected_calculate_primary_service_rolling_sums_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.unix_time,
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_rolling_current_sum_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_current_sum],
                self.expected_data[i][job.TempCol.rolling_current_sum],
                2,
                f"Returned row {i} does not match expected",
            )

    def test_returned_rolling_previous_sum_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.rolling_previous_sum],
                self.expected_data[i][job.TempCol.rolling_previous_sum],
                2,
                f"Returned row {i} does not match expected",
            )
