import unittest
from unittest.mock import Mock, patch
import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_longitudinal_outliers as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    OutlierCleaningData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    OutlierCleaningSchemas as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_longitudinal_outliers"
)


class TestCleanLongitudinalOutliers(unittest.TestCase):
    def setUp(self):
        self.test_lf = pl.LazyFrame(
            Data.clean_longitudinal_outliers_input_rows,
            Schemas.input_schema,
            orient="row",
        )


class TestFunctionsAreCalled(TestCleanLongitudinalOutliers):
    @patch(f"{PATCH_PATH}.compute_outlier_cutoff_and_clean")
    @patch(f"{PATCH_PATH}.update_filtering_rule")
    def test_functions_are_called(
        self,
        update_filtering_rule_mock: Mock,
        compute_outlier_cutoff_and_clean_mock: Mock,
    ):
        job.clean_longitudinal_outliers(
            lf=self.test_lf,
            col_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            proportion_to_filter=0.10,
            care_home=False,
        )

        compute_outlier_cutoff_and_clean_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()


class TestCleanLongitudinalOutliersValues(TestCleanLongitudinalOutliers):
    def test_function_returns_expected_values(self):
        returned_lf = job.clean_longitudinal_outliers(
            lf=self.test_lf,
            col_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            proportion_to_filter=0.10,
            care_home=False,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_clean_longitudinal_outliers_remove_value_only_rows,
            Schemas.input_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestComputeOutlierCutoffAndClean(unittest.TestCase):
    def setUp(self):
        self.test_lf = pl.LazyFrame(
            Data.compute_outlier_cutoff_and_clean_input_rows,
            Schemas.compute_outlier_cutoff_and_clean_input_schema,
            orient="row",
        )
        self.returned_lf = job.compute_outlier_cutoff_and_clean(
            lf=self.test_lf,
            col_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            proportion_to_filter=0.10,
        )

        self.expected_lf = pl.LazyFrame(
            Data.expected_compute_outlier_cutoff_and_clean_rows,
            Schemas.compute_outlier_cutoff_and_clean_output_schema,
            orient="row",
        )

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf, self.expected_lf, check_row_order=False
        )
