import unittest
from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_repetition as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanCtRepetition as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanCtRepetition as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_repetition"
)


class CleanCtRepetitionTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class FunctionConstantsTests(CleanCtRepetitionTests):
    def test_function_constants_are_correct(self):
        self.assertEqual(job.POSTS_TO_DEFINE_LARGE_PROVIDER, 50)
        self.assertEqual(job.LARGE_PROVIDER, "large provider")
        self.assertEqual(job.REPETITION_LIMIT_ALL_PROVIDERS, 365)
        self.assertEqual(job.REPETITION_LIMIT_LARGE_PROVIDER, 185)


class NullCTValuesAfterConsecutiveRepetition(CleanCtRepetitionTests):
    def setUp(self):
        super().setUp()

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    @patch(f"{PATCH_PATH}.clean_capacity_tracker_posts_repetition")
    @patch(f"{PATCH_PATH}.calculate_days_a_provider_has_been_repeating_values")
    @patch(f"{PATCH_PATH}.create_column_with_repeated_values_removed")
    @patch(f"{PATCH_PATH}.aggregate_values_to_provider_level")
    def test_null_ct_values_after_consecutive_repetition_has_correct_function_calls(
        self,
        aggregate_values_to_provider_level_mock: Mock,
        create_column_with_repeated_values_removed_mock: Mock,
        calculate_days_a_provider_has_been_repeating_values_mock: Mock,
        clean_capacity_tracker_posts_repetition_mock: Mock,
        update_filtering_rule_mock: Mock,
    ):

        test_df = self.spark.createDataFrame(
            Data.null_ct_values_after_consec_rep_with_provider_repetition_outside_limit_and_providers_are_small_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )

        job.null_ct_values_after_consecutive_repetition(
            test_df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )

        aggregate_values_to_provider_level_mock.assert_called_once()
        create_column_with_repeated_values_removed_mock.assert_called_once()
        calculate_days_a_provider_has_been_repeating_values_mock.assert_called_once()
        clean_capacity_tracker_posts_repetition_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()

    def test_null_ct_values_after_consecutive_repetition_nulls_repeated_values_with_provider_repetition_outside_limit_and_providers_are_small(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_ct_values_after_consec_rep_with_provider_repetition_outside_limit_and_providers_are_small_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )
        returned_df = job.null_ct_values_after_consecutive_repetition(
            test_df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_null_ct_values_after_consec_rep_with_provider_repetition_outside_limit_and_providers_are_small_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_null_ct_values_after_consecutive_repetition_returns_input_values_without_provider_repetition_outside_limit_and_providers_are_small(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_ct_values_after_consec_rep_without_provider_repetition_outside_limit_and_providers_are_small_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )
        returned_df = job.null_ct_values_after_consecutive_repetition(
            test_df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_null_ct_values_after_consec_rep_without_provider_repetition_outside_limit_and_providers_are_small_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_null_ct_values_after_consecutive_repetition_nulls_repeated_values_with_provider_repetition_outside_limit_and_providers_are_large(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_ct_values_after_consec_rep_with_provider_repetition_outside_limit_and_providers_are_large_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )
        returned_df = job.null_ct_values_after_consecutive_repetition(
            test_df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_null_ct_values_after_consec_rep_with_provider_repetition_outside_limit_and_providers_are_large_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_null_ct_values_after_consecutive_repetition_returns_input_values_without_provider_repetition_outside_limit_and_providers_are_large(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_ct_values_after_consec_rep_without_provider_repetition_outside_limit_and_providers_are_large_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )
        returned_df = job.null_ct_values_after_consecutive_repetition(
            test_df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_null_ct_values_after_consec_rep_without_provider_repetition_outside_limit_and_providers_are_large_rows,
            Schemas.null_ct_values_after_consec_rep_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class AggregateValuesToProviderLevel(CleanCtRepetitionTests):
    def setUp(self):
        super().setUp()

    def test_aggregate_values_to_provider_level_returns_expected_values(self):
        test_df = self.spark.createDataFrame(
            Data.aggregate_values_to_provider_level_rows,
            Schemas.aggregate_values_to_provider_level_schema,
        )
        returned_df = job.aggregate_values_to_provider_level(
            test_df, IndCQC.ct_care_home_total_employed_cleaned
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_values_to_provider_level_rows,
            Schemas.expected_aggregate_values_to_provider_level_schema,
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(returned_data, expected_data)


class CalculateDaysAProviderHasBeenRepeatingValues(CleanCtRepetitionTests):
    def setUp(self):
        super().setUp()

    def test_calculate_days_a_provider_has_been_repeating_values_returns_expected_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_days_a_provider_has_been_repeating_values_rows,
            Schemas.calculate_days_a_provider_has_been_repeating_values_schema,
        )
        returned_df = job.calculate_days_a_provider_has_been_repeating_values(
            test_df, IndCQC.ct_care_home_total_employed_cleaned_provider_sum_dedupicated
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_days_a_provider_has_been_repeating_values_rows,
            Schemas.expected_calculate_days_a_provider_has_been_repeating_values_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class CleanCapacityTrackerPostsRepetition(CleanCtRepetitionTests):
    def setUp(self):
        super().setUp()

    def test_clean_capacity_tracker_posts_repetition_returns_expected_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_capacity_tracker_posts_repetition_rows,
            Schemas.clean_capacity_tracker_posts_repetition_schema,
        )
        returned_df = job.clean_capacity_tracker_posts_repetition(
            test_df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_capacity_tracker_posts_repetition_rows,
            Schemas.clean_capacity_tracker_posts_repetition_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())
