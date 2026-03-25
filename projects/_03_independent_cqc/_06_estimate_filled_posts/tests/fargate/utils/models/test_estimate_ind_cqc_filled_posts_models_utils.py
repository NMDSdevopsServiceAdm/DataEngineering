import unittest
from unittest.mock import ANY, Mock, patch
import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models import (
    utils as job,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsModelsUtils as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsModelsUtils as Schemas,
)

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.utils"
)


class EnrichWithModelPredictionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_bucket = "test_bucket"

        self.mock_ind_cqc_lf = Mock(name="ind_cqc_lf")
        self.mock_predictions_lf = Mock(name="predictions_lf")

        self.ind_cqc_lf = pl.LazyFrame(
            Data.enrich_model_ind_cqc_rows,
            Schemas.enrich_model_ind_cqc_schema,
            orient="row",
        )

        self.care_home_model = Schemas.test_care_home_model_name
        self.care_home_pred_lf = pl.LazyFrame(
            Data.enrich_model_predictions_care_home_rows,
            Schemas.enrich_model_predictions_care_home_schema,
            orient="row",
        )
        self.expected_enriched_care_home_lf = pl.LazyFrame(
            Data.expected_enrich_model_ind_cqc_care_home_rows,
            Schemas.expected_enrich_model_ind_cqc_care_home_schema,
            orient="row",
        )

        self.non_res_model = Schemas.test_non_res_model_name
        self.non_res_pred_lf = pl.LazyFrame(
            Data.enrich_model_predictions_non_res_rows,
            Schemas.enrich_model_predictions_non_res_schema,
            orient="row",
        )
        self.expected_enriched_non_res_lf = pl.LazyFrame(
            Data.expected_enrich_model_ind_cqc_non_res_rows,
            Schemas.expected_enrich_model_ind_cqc_non_res_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.join_model_predictions")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_calls_all_necessary_functions_when_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        scan_parquet_mock: Mock,
        join_model_predictions_mock: Mock,
    ):
        scan_parquet_mock.return_value = self.mock_predictions_lf

        job.enrich_with_model_predictions(
            self.mock_ind_cqc_lf, self.test_bucket, self.care_home_model
        )

        generate_predictions_path_mock.assert_called_once_with(
            self.test_bucket, self.care_home_model
        )
        scan_parquet_mock.assert_called_once_with(
            generate_predictions_path_mock.return_value
        )
        join_model_predictions_mock.assert_called_once_with(
            ANY, ANY, self.care_home_model, include_run_id=True
        )

    @patch(f"{PATCH_PATH}.join_model_predictions")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_does_not_call_ratio_conversion_for_non_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        scan_parquet_mock: Mock,
        join_model_predictions_mock: Mock,
    ):
        scan_parquet_mock.return_value = self.mock_predictions_lf

        job.enrich_with_model_predictions(
            self.mock_ind_cqc_lf, self.test_bucket, self.non_res_model
        )

        generate_predictions_path_mock.assert_called_once_with(
            self.test_bucket, self.non_res_model
        )
        scan_parquet_mock.assert_called_once_with(
            generate_predictions_path_mock.return_value
        )
        join_model_predictions_mock.assert_called_once_with(
            ANY, ANY, self.non_res_model, include_run_id=True
        )

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_returns_expected_data_for_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        scan_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = self.care_home_pred_lf

        returned_lf = job.enrich_with_model_predictions(
            self.ind_cqc_lf, self.test_bucket, self.care_home_model
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_enriched_care_home_lf, check_row_order=False
        )

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_returns_expected_data_for_non_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        scan_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = self.non_res_pred_lf

        returned_lf = job.enrich_with_model_predictions(
            self.ind_cqc_lf, self.test_bucket, self.non_res_model
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_enriched_non_res_lf, check_row_order=False
        )


class JoinModelPredictionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.model_name = Schemas.join_test_model

        ind_cqc_lf = pl.LazyFrame(
            Data.join_ind_cqc_rows,
            Schemas.join_ind_cqc_schema,
            orient="row",
        )
        predictions_lf = pl.LazyFrame(
            Data.join_prediction_rows,
            Schemas.join_prediction_schema,
            orient="row",
        )

        self.returned_without_run_id_lf = job.join_model_predictions(
            ind_cqc_lf, predictions_lf, self.model_name, include_run_id=False
        )
        self.expected_without_run_id_lf = pl.LazyFrame(
            Data.expected_join_without_run_id_rows,
            Schemas.expected_join_without_run_id_schema,
            orient="row",
        )

        self.returned_with_run_id_lf = job.join_model_predictions(
            ind_cqc_lf, predictions_lf, self.model_name, include_run_id=True
        )
        self.expected_with_run_id_lf = pl.LazyFrame(
            Data.expected_join_with_run_id_rows,
            Schemas.expected_join_with_run_id_schema,
            orient="row",
        )

    def test_returns_expected_values_when_include_run_id_is_false(self):

        pl_testing.assert_frame_equal(
            self.returned_without_run_id_lf,
            self.expected_without_run_id_lf,
            check_row_order=False,
        )

    def test_returns_expected_values_when_include_run_id_is_true(self):

        pl_testing.assert_frame_equal(
            self.returned_with_run_id_lf,
            self.expected_with_run_id_lf,
            check_row_order=False,
        )
