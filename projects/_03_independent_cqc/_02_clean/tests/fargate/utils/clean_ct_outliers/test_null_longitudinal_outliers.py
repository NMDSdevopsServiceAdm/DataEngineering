import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.null_longitudinal_outliers as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    NullLongitudinalOutliersData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    NullLongitudinalOutliersSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.null_longitudinal_outliers"
)


class NullLongitudinalOutliersTests(unittest.TestCase):
    def setUp(self):
        self.test_lf = pl.LazyFrame(
            Data.null_longitudinal_outliers_input_rows,
            Schemas.input_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    def test_functions_are_called(
        self,
        update_filtering_rule_mock: Mock,
    ):
        job.null_longitudinal_outliers(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            proportion_to_filter=0.10,
            care_home=False,
        )
        update_filtering_rule_mock.assert_called_once()

    def test_function_returns_expected_values(self):
        returned_lf = job.null_longitudinal_outliers(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            proportion_to_filter=0.10,
            care_home=False,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_null_longitudinal_outliers_remove_value_only_rows,
            Schemas.input_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
