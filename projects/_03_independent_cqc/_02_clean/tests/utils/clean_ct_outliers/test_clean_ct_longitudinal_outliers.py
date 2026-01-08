import unittest
from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_longitudinal_outliers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    OutlierCleaningData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    OutlierCleaningSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_longitudinal_outliers"
)


class TestCleanCtLongitudinalOutliers(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.test_df = self.spark.createDataFrame(
            Data.clean_longitudinal_outliers_input_rows, Schemas.input_schema
        )


class TestFunctionsAreCalled(TestCleanCtLongitudinalOutliers):
    @patch(f"{PATCH_PATH}.compute_group_median")
    @patch(f"{PATCH_PATH}.calculate_new_column")
    @patch(f"{PATCH_PATH}.compute_outlier_cutoff")
    @patch(f"{PATCH_PATH}.apply_outlier_cleaning")
    @patch(f"{PATCH_PATH}.update_filtering_rule")
    def test_functions_are_called(
        self,
        update_filtering_rule_mock: Mock,
        apply_outlier_cleaning_mock: Mock,
        calculate_new_column_mock: Mock,
        compute_absolute_deviation_mock: Mock,
        compute_group_median_mock: Mock,
    ):
        job.clean_longitudinal_outliers(
            self.test_df,
            IndCQC.location_id,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_care_home_total_employed_cleaned,
            0.10,
            True,
        )

        compute_group_median_mock.assert_called_once()
        compute_absolute_deviation_mock.assert_called_once()
        calculate_new_column_mock.assert_called_once()
        apply_outlier_cleaning_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()


class TestRemoveCTValueOutliers(TestCleanCtLongitudinalOutliers):
    def setUp(self) -> None:
        super().setUp()

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
            col_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            proportion_to_filter=0.10,
            care_home=False,
        )
        self.assertEqual(returned_df.collect(), test_df.collect())

    def test_clean_longitudinal_outliers_nulls_outlier_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_longitudinal_outliers_input_rows,
            Schemas.input_schema,
        )

        returned_df = job.clean_longitudinal_outliers(
            test_df,
            IndCQC.location_id,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            0.10,
            False,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_longitudinal_outliers_remove_value_only_rows,
            Schemas.input_schema,
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
            IndCQC.ct_non_res_care_workers_employed_cleaned,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_compute_group_median_rows,
            Schemas.median_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestComputeOutlierCutoff(TestCleanCtLongitudinalOutliers):
    def test_compute_outlier_cutoff_returns_expected_values(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.compute_outlier_cutoff_rows,
            Schemas.compute_outlier_cutoff_input_schema,
        )

        returned_data = job.compute_outlier_cutoff(
            df,
            0.10,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
        ).collect()

        expected_data = self.spark.createDataFrame(
            Data.expected_outlier_cutoff_rows,
            Schemas.cutoff_schema,
        ).collect()

        for i in range(len(expected_data)):
            self.assertAlmostEqual(
                returned_data[i][
                    f"{IndCQC.ct_non_res_care_workers_employed_cleaned}_overall_abs_diff_cutoff"
                ],
                expected_data[i][
                    f"{IndCQC.ct_non_res_care_workers_employed_cleaned}_overall_abs_diff_cutoff"
                ],
                places=3,
            )


class TestApplyOutlierCleaning(TestCleanCtLongitudinalOutliers):
    def test_apply_outlier_cleaning_nulls_outlier_values(
        self,
    ):
        df = self.spark.createDataFrame(
            Data.apply_outlier_cleaning_input_rows,
            Schemas.apply_outlier_cleaning_schema,
        )

        returned_df = job.apply_outlier_cleaning(
            df,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_apply_outlier_cleaning_input_rows,
            Schemas.apply_outlier_cleaning_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())
