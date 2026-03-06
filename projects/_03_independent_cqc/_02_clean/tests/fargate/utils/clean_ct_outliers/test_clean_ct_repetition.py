import unittest
from unittest.mock import Mock, patch
import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanCtRepetition as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanCtRepetition as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition"
)


class NullCTValuesAfterConsecutiveRepetition(unittest.TestCase):
    def setUp(self):
        self.test_lf = pl.LazyFrame(
            Data.clean_ct_values_after_consecutive_repetition_rows,
            Schemas.clean_ct_values_after_consecutive_repetition_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    @patch(f"{PATCH_PATH}.clean_value_repetition")
    @patch(f"{PATCH_PATH}.calculate_days_a_value_has_been_repeated")
    @patch(f"{PATCH_PATH}.create_column_with_repeated_values_removed")
    def test_function_has_correct_sub_function_calls(
        self,
        create_column_with_repeated_values_removed_mock: Mock,
        calculate_days_a_value_has_been_repeated_mock: Mock,
        clean_value_repetition_mock: Mock,
        update_filtering_rule_mock: Mock,
    ):
        job.clean_ct_values_after_consecutive_repetition(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
            partitioning_column=IndCQC.location_id,
        )

        create_column_with_repeated_values_removed_mock.assert_called_once()
        calculate_days_a_value_has_been_repeated_mock.assert_called_once()
        clean_value_repetition_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()

    def test_function_returns_expected_values(self):
        returned_lf = job.clean_ct_values_after_consecutive_repetition(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
            partitioning_column=IndCQC.location_id,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_clean_ct_values_after_consecutive_repetition_rows,
            Schemas.expected_clean_ct_values_after_consecutive_repetition_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(IndCQC.location_id, IndCQC.cqc_location_import_date),
            expected_lf.sort(IndCQC.location_id, IndCQC.cqc_location_import_date),
            check_column_order=False,
        )

    def test_dict_of_minimum_posts_and_max_repetition_days_values_are_correct(self):
        self.assertEqual(
            job.DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES,
            Data.expected_dict_non_residential_locations,
        )
        self.assertEqual(
            job.DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES,
            Data.expected_dict_care_home_locations,
        )


class CalculateDaysAValueHasBeenRepeated(unittest.TestCase):
    def setUp(self):
        self.test_lf = pl.LazyFrame(
            Data.calculate_days_a_value_has_been_repeated_rows,
            Schemas.calculate_days_a_value_has_been_repeated_schema,
            orient="row",
        )
        self.returned_lf = job.calculate_days_a_value_has_been_repeated(
            lf=self.test_lf,
            deduplicated_values_column="values_deduplicated",
            partitioning_column=IndCQC.location_id,
        )
        self.expected_lf = pl.LazyFrame(
            Data.expected_calculate_days_a_value_has_been_repeated_rows,
            Schemas.expected_calculate_days_a_value_has_been_repeated_schema,
            orient="row",
        )

    def test_calculate_days_a_value_has_been_repeated_adds_1_column(self):
        returned_cols = self.returned_lf.collect_schema().names()
        test_cols = self.test_lf.collect_schema().names()
        new_cols = [col for col in returned_cols if col not in test_cols]
        self.assertEqual(len(new_cols), 1)
        self.assertEqual(new_cols[0], "days_value_has_been_repeated")

    def test_calculate_days_a_value_has_been_repeated_returns_expected_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(IndCQC.location_id, IndCQC.cqc_location_import_date),
            self.expected_lf.sort(IndCQC.location_id, IndCQC.cqc_location_import_date),
        )


class CleanValueRepetition(unittest.TestCase):
    def setUp(self):
        test_repetition_limit_dict = Data.test_repetition_limit_dict

        self.test_micro_lf = pl.LazyFrame(
            Data.clean_value_repetition_when_location_is_micro_rows,
            Schemas.clean_value_repetition_schema,
            orient="row",
        )
        self.test_small_lf = pl.LazyFrame(
            Data.clean_value_repetition_when_location_is_small_rows,
            Schemas.clean_value_repetition_schema,
            orient="row",
        )
        self.test_medium_lf = pl.LazyFrame(
            Data.clean_value_repetition_when_location_is_medium_rows,
            Schemas.clean_value_repetition_schema,
            orient="row",
        )
        self.test_large_lf = pl.LazyFrame(
            Data.clean_value_repetition_when_location_is_large_rows,
            Schemas.clean_value_repetition_schema,
            orient="row",
        )

        self.returned_micro_lf = job.clean_value_repetition(
            lf=self.test_micro_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )
        self.returned_small_lf = job.clean_value_repetition(
            lf=self.test_small_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )
        self.returned_medium_lf = job.clean_value_repetition(
            lf=self.test_medium_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )
        self.returned_large_lf = job.clean_value_repetition(
            lf=self.test_large_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            repetition_limit_dict=test_repetition_limit_dict,
        )

        self.expected_micro_lf = pl.LazyFrame(
            Data.expected_clean_value_repetition_when_location_is_micro_rows,
            Schemas.expected_clean_value_repetition_schema,
            orient="row",
        )
        self.expected_small_lf = pl.LazyFrame(
            Data.expected_clean_value_repetition_when_location_is_small_rows,
            Schemas.expected_clean_value_repetition_schema,
            orient="row",
        )
        self.expected_medium_lf = pl.LazyFrame(
            Data.expected_clean_value_repetition_when_location_is_medium_rows,
            Schemas.expected_clean_value_repetition_schema,
            orient="row",
        )
        self.expected_large_lf = pl.LazyFrame(
            Data.expected_clean_value_repetition_when_location_is_large_rows,
            Schemas.expected_clean_value_repetition_schema,
            orient="row",
        )

    def test_clean_value_repetition_adds_1_column(self):
        returned_cols = self.returned_micro_lf.collect_schema().names()
        test_cols = self.test_micro_lf.collect_schema().names()
        new_cols = [col for col in returned_cols if col not in test_cols]
        self.assertEqual(len(new_cols), 1)
        self.assertEqual(new_cols[0], "repeated_values_nulled")

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_micro_locations(
        self,
    ):
        pl_testing.assert_frame_equal(
            self.returned_micro_lf,
            self.expected_micro_lf,
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_small_locations(
        self,
    ):
        pl_testing.assert_frame_equal(
            self.returned_small_lf,
            self.expected_small_lf,
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_medium_locations(
        self,
    ):
        pl_testing.assert_frame_equal(
            self.returned_medium_lf,
            self.expected_medium_lf,
        )

    def test_clean_value_repetition_nulls_values_when_repetition_days_above_limit_at_large_locations(
        self,
    ):
        pl_testing.assert_frame_equal(
            self.returned_large_lf,
            self.expected_large_lf,
        )
