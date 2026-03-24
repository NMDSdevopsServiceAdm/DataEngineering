import unittest
from unittest.mock import Mock, patch
import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    clean_ct_repetition_values_test_cases,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanCtRepetition as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition"
)


class CleanCTValuesAfterConsecutiveRepetitionTests(unittest.TestCase):
    def setUp(self):
        self.test_lf = Mock(name="ind_cqc_df")

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    @patch(f"{PATCH_PATH}.repetition_limit_expr")
    def test_function_has_correct_sub_function_calls(
        self,
        repetition_limit_expr_mock: Mock,
        update_filtering_rule_mock: Mock,
    ):
        job.clean_ct_values_after_consecutive_repetition(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
        )

        repetition_limit_expr_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()


class TestSomething:
    @pytest.fixture(
        params=[
            pytest.param(case.data, id=case.id)
            for case in clean_ct_repetition_values_test_cases
        ],
    )
    def clean_ct_case_data(self, request):
        return request.param

    def test_function_returns_expected_values(self, clean_ct_case_data):
        expected_lf = pl.LazyFrame(
            data=clean_ct_case_data, schema=Schemas.clean_ct_values_schema, orient="row"
        )
        input_lf = expected_lf.drop(IndCQC.ct_non_res_care_workers_employed_cleaned)
        returned_lf = job.clean_ct_values_after_consecutive_repetition(
            lf=input_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed,
            cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
        )
        print(returned_lf.collect())
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, check_row_order=False
        )
