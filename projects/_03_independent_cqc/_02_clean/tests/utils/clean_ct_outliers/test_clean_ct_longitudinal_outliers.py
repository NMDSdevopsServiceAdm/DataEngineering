import unittest

import projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_longitudinal_outliers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    OutlierCleaningData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    OutlierCleaningSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestCleanCtLongitudinalOutliers(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class TestRemoveCTValueOutliers(TestCleanCtLongitudinalOutliers):
    def setUp(self) -> None:
        super().setUp()

    def test_clean_longitudinal_outliers_removes_outlier_rows_when_remove_whole_record_is_true(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_random_spikes_input_rows,
            Schemas.input_schema,
        )

        returned_df = job.clean_longitudinal_outliers(
            test_df,
            IndCQC.location_id,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            0.10,
            True,
            True,
        ).select(
            IndCQC.location_id,
            IndCQC.ct_care_home_total_employed_cleaned,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_random_spikes_remove_whole_rows,
            Schemas.cleaned_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_clean_longitudinal_outliers_returns_input_df_when_there_are_no_outliers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.no_outliers_input_rows,
            Schemas.input_schema,
        )

        returned_df = job.clean_longitudinal_outliers(
            df=test_df,
            group_by_col=IndCQC.location_id,
            col_to_clean=IndCQC.ct_care_home_total_employed_cleaned,
            cleaned_column_name=IndCQC.ct_care_home_total_employed_cleaned,
            proportion_to_filter=0.10,
            remove_whole_record=False,
            care_home=True,
        )
        print("returned df is: ", returned_df.collect())
        self.assertEqual(returned_df.collect(), test_df.collect())

    def test_clean_longitudinal_outliers_nulls_outlier_values_when_remove_whole_record_is_false(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_random_spikes_input_rows,
            Schemas.input_schema,
        )

        returned_df = job.clean_longitudinal_outliers(
            test_df,
            IndCQC.location_id,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            0.10,
            False,
            True,
        ).select(
            IndCQC.location_id,
            IndCQC.ct_care_home_total_employed_cleaned,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_clean_random_spikes_remove_value_only_rows,
            Schemas.cleaned_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestComputeMedian(TestCleanCtLongitudinalOutliers):
    def test_compute_group_median_returns_expected_values(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.compute_group_median_rows,
            Schemas.input_schema,
        )

        returned_df = job.compute_group_median(
            df,
            IndCQC.location_id,
            IndCQC.ct_care_home_total_employed_cleaned,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_compute_group_median_rows,
            Schemas.median_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestComputeAbsDeviation(TestCleanCtLongitudinalOutliers):
    def test_compute_absolute_deviation(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.compute_abs_deviation_rows,
            Schemas.median_schema,
        )

        returned_df = job.compute_absolute_deviation(
            df, IndCQC.ct_care_home_total_employed_cleaned, "median_val"
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_abs_deviation_rows,
            Schemas.abs_dev_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestComputeMad(TestCleanCtLongitudinalOutliers):
    def test_compute_mad_returns_expected_values(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.compute_mad_rows,
            Schemas.abs_dev_schema,
        )

        returned_df = job.compute_mad(df, IndCQC.location_id)

        expected_df = self.spark.createDataFrame(
            Data.expected_mad_rows,
            Schemas.mad_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestComputeOutlierCutoff(TestCleanCtLongitudinalOutliers):
    def test_compute_outlier_cutoff_returns_expected_values(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.compute_outlier_cutoff_rows,
            Schemas.mad_schema,
        )

        returned_df = job.compute_outlier_cutoff(df, IndCQC.location_id, 0.10)

        expected_df = self.spark.createDataFrame(
            Data.expected_outlier_cutoff_rows,
            Schemas.cutoff_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestFlagOutliers(TestCleanCtLongitudinalOutliers):
    def test_flag_outliers_returns_expected_values(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.flag_outliers_rows,
            Schemas.cutoff_schema,
        )

        returned_df = job.flag_outliers(df)

        expected_df = self.spark.createDataFrame(
            Data.expected_flag_outliers_rows,
            Schemas.expected_outlier_flags_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestApplyOutlierCleaning(TestCleanCtLongitudinalOutliers):
    def test_apply_outlier_cleaning_nulls_outlier_values_when_remove_whole_record_is_false(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.apply_outlier_cleaning_input_rows,
            Schemas.apply_outlier_cleaning_input_schema,
        )

        returned_df = job.apply_outlier_cleaning(
            df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            False,
        )

        expected_df = self.spark.createDataFrame(
            Data.apply_outlier_cleaning_expected_rows,
            Schemas.final_cleaned_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_apply_outlier_cleaning_removes_outlier_rows_when_remove_whole_record_is_true(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.apply_outlier_cleaning_input_rows,
            Schemas.apply_outlier_cleaning_input_schema,
        )

        returned_df = job.apply_outlier_cleaning(
            df,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            True,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_apply_outlier_cleaning_when_removing_outlier_rows,
            Schemas.final_cleaned_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())
