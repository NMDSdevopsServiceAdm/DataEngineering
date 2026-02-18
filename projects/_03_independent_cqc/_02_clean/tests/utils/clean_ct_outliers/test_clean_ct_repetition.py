from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_repetition as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanCtRepetition as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanCtRepetition as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_repetition"
)


class CleanCTRepetitionTests(SparkBaseTest):
    def setUp(self): ...


class NullCTValuesAfterConsecutiveRepetition(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.clean_ct_values_after_consecutive_repetition_rows,
            Schemas.clean_ct_values_after_consecutive_repetition_schema,
        )

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    @patch(f"{PATCH_PATH}.join_cleaned_ct_values_into_original_df")
    @patch(f"{PATCH_PATH}.clean_value_repetition")
    @patch(f"{PATCH_PATH}.calculate_days_a_value_has_been_repeated")
    @patch(f"{PATCH_PATH}.create_column_with_repeated_values_removed")
    def test_clean_ct_values_after_consecutive_repetition_has_correct_function_calls(
        self,
        create_column_with_repeated_values_removed_mock: Mock,
        calculate_days_a_value_has_been_repeated: Mock,
        clean_value_repetition: Mock,
        join_cleaned_ct_values_into_original_df_mock: Mock,
        update_filtering_rule_mock: Mock,
    ):

        job.clean_ct_values_after_consecutive_repetition(
            df=self.test_df,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
            partitioning_column=IndCQC.location_id,
        )

        create_column_with_repeated_values_removed_mock.assert_called_once()
        calculate_days_a_value_has_been_repeated.assert_called_once()
        clean_value_repetition.assert_called_once()
        join_cleaned_ct_values_into_original_df_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()

    def test_clean_ct_values_after_consecutive_repetition_returns_expected_values(
        self,
    ):
        returned_df = job.clean_ct_values_after_consecutive_repetition(
            df=self.test_df,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
            partitioning_column=IndCQC.location_id,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_ct_values_after_consecutive_repetition_rows,
            Schemas.expected_clean_ct_values_after_consecutive_repetition_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_dict_of_minimum_posts_and_max_repetition_days_values_are_correct(
        self,
    ):
        self.assertEqual(
            job.DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES,
            Data.expected_dict_non_residential_locations,
        )
        self.assertEqual(
            job.DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES,
            Data.expected_dict_care_home_locations,
        )


class CalculateDaysAValueHasBeenRepeated(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_days_a_value_has_been_repeated_rows,
            Schemas.calculate_days_a_value_has_been_repeated_schema,
        )
        self.returned_df = job.calculate_days_a_value_has_been_repeated(
            df=self.test_df,
            deduplicated_values_column="values_deduplicated",
            partitioning_column=IndCQC.location_id,
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
        self.assertEqual(new_cols[0], "days_value_has_been_repeated")

    def test_calculate_days_a_value_has_been_repeated_returns_expected_values(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class CleanValueRepetition(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        test_repetition_limit_dict = Data.test_repetition_limit_dict
        self.test_micro_locations_df = self.spark.createDataFrame(
            Data.clean_value_repetition_when_location_is_micro_rows,
            Schemas.clean_value_repetition_schema,
        )
        self.test_small_locations_df = self.spark.createDataFrame(
            Data.clean_value_repetition_when_location_is_small_rows,
            Schemas.clean_value_repetition_schema,
        )
        self.test_medium_locations_df = self.spark.createDataFrame(
            Data.clean_value_repetition_when_location_is_medium_rows,
            Schemas.clean_value_repetition_schema,
        )
        self.test_large_locations_df = self.spark.createDataFrame(
            Data.clean_value_repetition_when_location_is_large_rows,
            Schemas.clean_value_repetition_schema,
        )

        self.returned_micro_locations_df = job.clean_value_repetition(
            df=self.test_micro_locations_df,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )
        self.returned_small_locations_df = job.clean_value_repetition(
            df=self.test_small_locations_df,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )
        self.returned_medium_locations_df = job.clean_value_repetition(
            df=self.test_medium_locations_df,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )
        self.returned_large_locations_df = job.clean_value_repetition(
            df=self.test_large_locations_df,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )

        self.expected_micro_locations_df = self.spark.createDataFrame(
            Data.expected_clean_value_repetition_when_location_is_micro_rows,
            Schemas.expected_clean_value_repetition_schema,
        )
        self.expected_small_locations_df = self.spark.createDataFrame(
            Data.expected_clean_value_repetition_when_location_is_small_rows,
            Schemas.expected_clean_value_repetition_schema,
        )
        self.expected_medium_locations_df = self.spark.createDataFrame(
            Data.expected_clean_value_repetition_when_location_is_medium_rows,
            Schemas.expected_clean_value_repetition_schema,
        )
        self.expected_large_locations_df = self.spark.createDataFrame(
            Data.expected_clean_value_repetition_when_location_is_large_rows,
            Schemas.expected_clean_value_repetition_schema,
        )

    def test_clean_value_repetition_adds_1_column(
        self,
    ):
        new_cols = [
            col
            for col in self.returned_micro_locations_df.columns
            if col not in self.test_micro_locations_df.columns
        ]
        self.assertEqual(len(new_cols), 1)
        self.assertEqual(new_cols[0], "repeated_values_nulled")

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_micro_locations(
        self,
    ):
        self.assertEqual(
            self.returned_micro_locations_df.collect(),
            self.expected_micro_locations_df.collect(),
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_small_locations(
        self,
    ):
        self.assertEqual(
            self.returned_small_locations_df.collect(),
            self.expected_small_locations_df.collect(),
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_medium_locations(
        self,
    ):
        self.assertEqual(
            self.returned_medium_locations_df.collect(),
            self.expected_medium_locations_df.collect(),
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_large_locations(
        self,
    ):
        self.assertEqual(
            self.returned_large_locations_df.collect(),
            self.expected_large_locations_df.collect(),
        )


class JoinCleanedCTValuesIntoOriginalDf(CleanCTRepetitionTests):
    def setUp(self):
        super().setUp()

        self.test_orginal_df = self.spark.createDataFrame(
            Data.original_rows,
            Schemas.original_schema,
        )
        self.test_populated_only_df = self.spark.createDataFrame(
            Data.populated_only_rows,
            Schemas.populated_only_schema,
        )
        self.returned_df = job.join_cleaned_ct_values_into_original_df(
            original_df=self.test_orginal_df,
            populated_only_df=self.test_populated_only_df,
            cleaned_column_name=IndCQC.ct_care_home_total_employed_cleaned,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_populated_only_joined_with_original_rows,
            Schemas.original_schema,
        )

    def test_join_cleaned_ct_values_into_original_df_replaces_cleaned_values_column(
        self,
    ):
        self.assertEqual(self.test_orginal_df.columns, self.expected_df.columns)

    def test_join_cleaned_ct_values_into_original_df_replaces_values_in_the_cleaned_values_column(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_join_cleaned_ct_values_into_original_df_does_not_add_any_rows_to_original_df(
        self,
    ):
        self.assertEqual(self.test_orginal_df.count(), self.expected_df.count())
