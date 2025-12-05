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


class CleanCTRepetitionTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class NullCTValuesAfterConsecutiveRepetition(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.clean_ct_values_after_consecutive_repetition_rows,
            Schemas.clean_ct_values_after_consecutive_repetition_schema,
        )

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    @patch(f"{PATCH_PATH}.clean_value_repetition")
    @patch(f"{PATCH_PATH}.calculate_days_a_value_has_been_repeated")
    @patch(f"{PATCH_PATH}.create_column_with_repeated_values_removed")
    def test_clean_ct_values_after_consecutive_repetition_has_correct_function_calls(
        self,
        create_column_with_repeated_values_removed_mock: Mock,
        calculate_days_a_value_has_been_repeated: Mock,
        clean_value_repetition: Mock,
        update_filtering_rule_mock: Mock,
    ):

        job.clean_ct_values_after_consecutive_repetition(
            self.test_df,
            IndCQC.ct_non_res_care_workers_employed,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            True,
            IndCQC.location_id,
        )

        create_column_with_repeated_values_removed_mock.assert_called_once()
        calculate_days_a_value_has_been_repeated.assert_called_once()
        clean_value_repetition.assert_called_once()
        update_filtering_rule_mock.assert_called_once()

    def test_clean_ct_values_after_consecutive_repetition_returns_expected_values(
        self,
    ):
        returned_df = job.clean_ct_values_after_consecutive_repetition(
            self.test_df,
            IndCQC.ct_non_res_care_workers_employed,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
            IndCQC.location_id,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_ct_values_after_consecutive_repetition_rows,
            Schemas.expected_clean_ct_values_after_consecutive_repetition_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_dict_of_minimum_posts_and_max_repetition_days_values_are_correct(
        self,
    ):
        expected_dict_non_residential_locations = {
            0: 250,
            10: 125,
            50: 65,
        }
        expected_dict_care_home_locations = {
            0: 370,
            10: 155,
            50: 125,
            250: 65,
        }

        self.assertEqual(
            job.DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES,
            expected_dict_non_residential_locations,
        )
        self.assertEqual(
            job.DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES,
            expected_dict_care_home_locations,
        )


class CalculateDaysAValueHasBeenRepeated(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_days_a_value_has_been_repeated_rows,
            Schemas.calculate_days_a_value_has_been_repeated_schema,
        )
        self.returned_df = job.calculate_days_a_value_has_been_repeated(
            self.test_df, "values_deduplicated", IndCQC.location_id
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_days_a_value_has_been_repeated_rows,
            Schemas.expected_calculate_days_a_value_has_been_repeated_schema,
        )

    def test_calculate_days_a_value_has_been_repeated_adds_1_column(
        self,
    ):
        new_cols = [
            col for col in self.returned_df.columns if col not in self.test_df.columns
        ]
        self.assertEqual(len(new_cols), 1)
        self.assertEqual(new_cols[0], IndCQC.days_value_has_been_repeated)

    def test_calculate_days_a_value_has_been_repeated_returns_expected_values(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class CleanValueRepetition(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        self.test_df_non_res_locations = self.spark.createDataFrame(
            Data.clean_capacity_tracker_posts_repetition_non_res_locations_rows,
            Schemas.clean_capacity_tracker_posts_repetition_non_res_locations_schema,
        )
        self.test_df_care_home_locations = self.spark.createDataFrame(
            Data.clean_capacity_tracker_posts_repetition_care_home_locations_rows,
            Schemas.clean_capacity_tracker_posts_repetition_care_home_locations_schema,
        )
        self.returned_df_non_res_locations = job.clean_value_repetition(
            self.test_df_non_res_locations,
            IndCQC.ct_non_res_care_workers_employed,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )
        self.returned_df_care_home_locations = job.clean_value_repetition(
            self.test_df_care_home_locations,
            IndCQC.ct_care_home_total_employed,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )
        self.expected_df_non_res_locations = self.spark.createDataFrame(
            Data.expected_clean_capacity_tracker_posts_repetition_non_res_locations_rows,
            Schemas.expected_clean_capacity_tracker_posts_repetition_non_res_locations_schema,
        )
        self.expected_df_care_home_locations = self.spark.createDataFrame(
            Data.expected_clean_capacity_tracker_posts_repetition_care_home_locations_rows,
            Schemas.expected_clean_capacity_tracker_posts_repetition_care_home_locations_schema,
        )

    def test_clean_value_repetition_adds_1_column(
        self,
    ):
        new_cols = [
            col
            for col in self.returned_df_non_res_locations.columns
            if col not in self.test_df_non_res_locations.columns
        ]
        self.assertEqual(len(new_cols), 1)
        self.assertEqual(new_cols[0], IndCQC.ct_non_res_care_workers_employed_cleaned)

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_non_res_locations(
        self,
    ):
        self.assertEqual(
            self.returned_df_non_res_locations.collect(),
            self.expected_df_non_res_locations.collect(),
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_care_home_locations(
        self,
    ):
        self.assertEqual(
            self.returned_df_care_home_locations.collect(),
            self.expected_df_care_home_locations.collect(),
        )
