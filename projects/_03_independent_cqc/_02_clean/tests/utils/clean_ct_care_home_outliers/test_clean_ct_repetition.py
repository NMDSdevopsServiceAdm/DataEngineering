import unittest

import projects._03_independent_cqc._02_clean.utils.clean_ct_care_home_outliers.clean_ct_repetition as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanCtRepetition as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanCtRepetition as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class CleanCtRepetitionTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class null_ct_values_after_consecutive_repetition(CleanCtRepetitionTests):
    def setUp(self):
        super().setUp()

    def test_null_values_after_consecutive_repetition_when_values_repeat_for_more_than_12_months(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_values_after_consec_rep_with_reps_outside_limit_rows,
            Schemas.null_values_after_consec_rep_schema,
        )
        returned_df = job.null_ct_values_after_consecutive_repetition(
            test_df, "column_to_clean"
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_null_values_after_consec_rep_with_reps_outside_limit_rows,
            Schemas.null_values_after_consec_rep_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_null_values_after_consecutive_repetition_when_values_do_not_repeat_for_more_than_12_months(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_values_after_consec_rep_with_repetition_but_without_reps_outside_limit_rows,
            Schemas.null_values_after_consec_rep_schema,
        )
        returned_df = job.null_ct_values_after_consecutive_repetition(
            test_df, "column_to_clean"
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_null_values_after_consec_rep_with_repetition_but_without_reps_outside_limit_rows,
            Schemas.null_values_after_consec_rep_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


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
            test_df, IndCQC.ct_care_home_total_employed_cleaned_dedup
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_days_a_provider_has_been_repeating_values_rows,
            Schemas.expected_calculate_days_a_provider_has_been_repeating_values_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())
