import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.null_values_exceeding_repetition_limit as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    null_values_exceeding_repetition_limit_test_cases,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    NullValuesExceedingRepetitionLimitSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.null_values_exceeding_repetition_limit"
)


class NullValuesExceedingRepetitionLimitSucceedsTests(unittest.TestCase):
    def setUp(self):
        self.test_lf = Mock(name="ind_cqc_df")

    @patch(f"{PATCH_PATH}.update_filtering_rule")
    @patch(f"{PATCH_PATH}.repetition_limit_expr")
    def test_function_has_correct_sub_function_calls(
        self,
        repetition_limit_expr_mock: Mock,
        update_filtering_rule_mock: Mock,
    ):
        job.null_values_exceeding_repetition_limit(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
        )

        repetition_limit_expr_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()


class TestNullValuesExceedingRepetitionLimitValues:
    @pytest.mark.parametrize(
        "input_case_data, expected_case_data",
        [
            (case.input_data, case.expected_data)
            for case in null_values_exceeding_repetition_limit_test_cases
        ],
        ids=[case.id for case in null_values_exceeding_repetition_limit_test_cases],
    )
    def test_function_returns_expected_values(
        self, input_case_data, expected_case_data
    ):
        input_lf = pl.LazyFrame(
            data=input_case_data, schema=Schemas.input_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            data=expected_case_data, schema=Schemas.input_schema, orient="row"
        )
        returned_lf = job.null_values_exceeding_repetition_limit(
            lf=input_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
