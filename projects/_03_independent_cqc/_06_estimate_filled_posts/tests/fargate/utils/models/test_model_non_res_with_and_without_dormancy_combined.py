import unittest
from unittest.mock import ANY, Mock, patch
import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.non_res_with_and_without_dormancy_combined as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelNonResWithAndWithoutDormancyCombinedRows as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelNonResWithAndWithoutDormancyCombinedSchemas as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    NonResWithAndWithoutDormancyCombinedColumns as NRModel_TempCol,
)

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.non_res_with_and_without_dormancy_combined"


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_lf = pl.LazyFrame(
            Data.estimated_posts_rows,
            Schemas.estimated_posts_schema,
            orient="row",
        )
        self.input_lf = self.expected_lf.drop(IndCQC.non_res_combined_model)

        self.returned_lf = job.combine_non_res_with_and_without_dormancy_models(
            self.input_lf
        )

    @patch(f"{PATCH_PATH}.join_model_predictions")
    @patch(f"{PATCH_PATH}.calculate_and_apply_residuals")
    @patch(f"{PATCH_PATH}.calculate_and_apply_model_ratios")
    @patch(f"{PATCH_PATH}.group_time_registered_to_six_month_bands")
    def test_models_runs(
        self,
        group_time_registered_mock: Mock,
        calculate_and_apply_model_ratios_mock: Mock,
        calculate_and_apply_residuals_mock: Mock,
        join_model_predictions_mock: Mock,
    ):
        job.combine_non_res_with_and_without_dormancy_models(self.input_lf)

        group_time_registered_mock.assert_called_once()
        calculate_and_apply_model_ratios_mock.assert_called_once()
        calculate_and_apply_residuals_mock.assert_called_once()
        join_model_predictions_mock.assert_called_once()

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf, self.expected_lf, check_row_order=False
        )


class GroupTimeRegisteredToSixMonthBandsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_lf = pl.LazyFrame(
            Data.expected_group_time_registered_to_six_month_bands_rows,
            Schemas.group_time_registered_to_six_month_bands_schema,
            orient="row",
        )
        test_lf = self.expected_lf.drop(
            NRModel_TempCol.time_registered_banded_and_capped
        )
        self.returned_lf = job.group_time_registered_to_six_month_bands(test_lf)

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf, self.expected_lf, check_row_order=False
        )


class CalculateAndApplyModelRatioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_lf = pl.LazyFrame(
            Data.calculate_and_apply_model_ratios_rows,
            Schemas.calculate_and_apply_model_ratios_schema,
            orient="row",
        )
        self.test_lf = self.expected_lf.drop(
            NRModel_TempCol.avg_with_dormancy,
            NRModel_TempCol.avg_without_dormancy,
            NRModel_TempCol.adjustment_ratio,
            NRModel_TempCol.non_res_without_dormancy_model_adjusted,
        )
        self.returned_lf = job.calculate_and_apply_model_ratios(self.test_lf)

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf,
            self.expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class CalculateAndApplyResidualsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_lf = pl.LazyFrame(
            Data.calculate_and_apply_residuals_rows,
            Schemas.calculate_and_apply_residuals_schema,
            orient="row",
        )
        self.test_lf = self.expected_lf.drop(
            NRModel_TempCol.residual_at_overlap,
            NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
        )
        self.returned_lf = job.calculate_and_apply_residuals(self.test_lf)

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf,
            self.expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class CalculateResidualsTests(unittest.TestCase):
    def setUp(self) -> None:
        test_lf = pl.LazyFrame(
            Data.calculate_residuals_rows,
            Schemas.calculate_residuals_schema,
            orient="row",
        )
        self.returned_lf = job.calculate_residuals(test_lf)
        self.expected_lf = pl.LazyFrame(
            Data.expected_calculate_residuals_rows,
            Schemas.expected_calculate_residuals_schema,
            orient="row",
        )

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class ApplyResidualsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_lf = pl.LazyFrame(
            Data.apply_residuals_rows, Schemas.apply_residuals_schema, orient="row"
        )
        test_lf = self.expected_lf.drop(
            NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied
        )
        self.returned_lf = job.apply_residuals(test_lf)

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)
