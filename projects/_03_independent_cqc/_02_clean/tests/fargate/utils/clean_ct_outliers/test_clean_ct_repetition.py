import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanCtRepetitionData as Data,
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
        job.null_values_exceeding_repetition_limit(
            lf=self.test_lf,
            column_to_clean=IndCQC.ct_non_res_care_workers_employed_cleaned,
            care_home=False,
        )

        repetition_limit_expr_mock.assert_called_once()
        update_filtering_rule_mock.assert_called_once()

    def test_when_values_repeat_after_a_missing_value_returns_nulled_value(self):
        test_lf = pl.LazyFrame(
            Data.values_repeat_after_a_missing_value,
            Schemas.clean_ct_repetition_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_values_repeat_after_a_missing_value,
            Schemas.expected_clean_ct_repetition_schema,
            orient="row",
        )
        returned_lf = job.null_values_exceeding_repetition_limit(
            test_lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_values_repeat_but_not_consecutively_returns_input_values(self):
        test_lf = pl.LazyFrame(
            Data.values_repeat_but_not_consecutively,
            Schemas.clean_ct_repetition_schema,
            orient="row",
        )
        expected_lf = test_lf
        returned_lf = job.null_values_exceeding_repetition_limit(
            test_lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_micro_location_repeats_after_too_long_returns_nulled_value(self):
        test_lf = pl.LazyFrame(
            Data.micro_location_repeats_after_too_long,
            Schemas.clean_ct_repetition_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_micro_location_repeats_after_too_long,
            Schemas.expected_clean_ct_repetition_schema,
            orient="row",
        )
        returned_lf = job.null_values_exceeding_repetition_limit(
            test_lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_small_location_repeats_after_too_long_returns_nulled_value(self):
        test_lf = pl.LazyFrame(
            Data.small_location_repeats_after_too_long,
            Schemas.clean_ct_repetition_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_small_location_repeats_after_too_long,
            Schemas.expected_clean_ct_repetition_schema,
            orient="row",
        )
        returned_lf = job.null_values_exceeding_repetition_limit(
            test_lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_medium_location_repeats_after_too_long_returns_nulled_value(self):
        test_lf = pl.LazyFrame(
            Data.medium_location_repeats_after_too_long,
            Schemas.clean_ct_repetition_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_medium_location_repeats_after_too_long,
            Schemas.expected_clean_ct_repetition_schema,
            orient="row",
        )
        returned_lf = job.null_values_exceeding_repetition_limit(
            test_lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_large_location_repeats_after_too_long_returns_nulled_value(self):
        test_lf = pl.LazyFrame(
            Data.large_location_repeats_after_too_long,
            Schemas.clean_ct_repetition_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_large_location_repeats_after_too_long,
            Schemas.expected_clean_ct_repetition_schema,
            orient="row",
        )
        returned_lf = job.null_values_exceeding_repetition_limit(
            test_lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            False,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
