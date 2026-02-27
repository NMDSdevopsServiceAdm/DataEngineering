from unittest.mock import Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change_cleaning as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelPrimaryServiceRateOfChangeCleaningData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelPrimaryServiceRateOfChangeCleaningSchemas as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as TempCol,
)

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change_cleaning"


class MainTests(SparkBaseTest):
    def setUp(self) -> None:

        self.input_df = Mock(name="input_df")

    @patch(f"{PATCH_PATH}.apply_rate_of_change_cleaning")
    @patch(f"{PATCH_PATH}.build_non_residential_keep_condition")
    @patch(f"{PATCH_PATH}.compute_non_residential_thresholds")
    @patch(f"{PATCH_PATH}.calculate_absolute_and_percentage_change")
    def test_main_calls_functions(
        self,
        calculate_change_mock: Mock,
        compute_thresholds_mock: Mock,
        build_condition_mock: Mock,
        apply_cleaning_mock: Mock,
    ):
        compute_thresholds_mock.return_value = (10, 1.25, 0.8)

        job.clean_non_residential_rate_of_change(self.input_df)

        calculate_change_mock.assert_called_once()
        compute_thresholds_mock.assert_called_once()
        build_condition_mock.assert_called_once()
        apply_cleaning_mock.assert_called_once()


class CalculateAbsoluteAndPercentageChangeTests(SparkBaseTest):
    def test_calculates_expected_changes(self):
        expected_df = self.spark.createDataFrame(
            Data.calculate_absolute_and_percentage_change_rows,
            Schemas.calculate_absolute_and_percentage_change_schema,
        )

        test_df = expected_df.drop(TempCol.abs_change, TempCol.perc_change)

        returned_df = job.calculate_absolute_and_percentage_change(
            test_df,
            TempCol.previous_period_interpolated,
            TempCol.current_period_interpolated,
        )

        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class ComputeNonResidentialThresholdTests(SparkBaseTest):
    def test_returns_expected_thresholds(self):
        test_cases = [
            ("all_rows_valid", Data.compute_non_res_threshold_valid_rows),
            ("invalid_rows_present", Data.compute_non_res_threshold_with_invalid_rows),
        ]

        for test_case_name, test_case_rows in test_cases:
            with self.subTest(test_case_name=test_case_name):
                self.test_df = self.spark.createDataFrame(
                    test_case_rows,
                    Schemas.compute_non_res_threshold_schema,
                )

                abs_upper, perc_upper, perc_lower = (
                    job.compute_non_residential_thresholds(
                        self.test_df,
                        TempCol.previous_period_interpolated,
                        TempCol.current_period_interpolated,
                        abs_percentile=0.5,
                        perc_percentile=0.5,
                    )
                )

                self.assertEqual(abs_upper, 5.0)
                self.assertEqual(perc_upper, 1.25)
                self.assertAlmostEqual(perc_lower, 1 / perc_upper)


class BuildNonResidentialKeepConditionTests(SparkBaseTest):
    def test_builds_expected_condition(self):
        expected_df = self.spark.createDataFrame(
            Data.build_keep_condition_rows,
            Schemas.build_keep_condition_schema,
        )

        test_df = expected_df.drop("keep")

        condition = job.build_non_residential_keep_condition(
            TempCol.previous_period_interpolated,
            TempCol.current_period_interpolated,
            abs_upper=50,
            perc_upper=2,
            perc_lower=0.5,
        )

        returned_df = test_df.withColumn("keep", condition)

        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class CalculateAbsoluteAndPercentageChangeTests(SparkBaseTest):
    def test_returns_expected_cleaned_values(self):

        expected_df = self.spark.createDataFrame(
            Data.apply_rate_of_change_cleaning_rows,
            Schemas.apply_rate_of_change_cleaning_schema,
        )

        test_df = expected_df.drop(
            TempCol.previous_period_cleaned, TempCol.current_period_cleaned
        )

        keep_condition = test_df["keep"]

        returned_df = job.apply_rate_of_change_cleaning(
            test_df,
            TempCol.previous_period_interpolated,
            TempCol.current_period_interpolated,
            keep_condition,
        )

        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)
