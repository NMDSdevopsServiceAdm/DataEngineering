import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.estimate_ind_cqc_filled_posts as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EstimateFilledPostsSource

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.estimate_ind_cqc_filled_posts"


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    TEST_BUCKET_NAME = "some/bucket/name"
    TEST_IMPUTED_IND_CQC_DATA_SOURCE = "some/s3/uri"
    TEST_DESTINATION = "some/other/s3/uri"

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.estimate_non_res_capacity_tracker_filled_posts")
    @patch(f"{PATCH_PATH}.set_min_value")
    @patch(f"{PATCH_PATH}.utils.coalesce_with_source_labels")
    @patch(f"{PATCH_PATH}.model_imputation")
    @patch(f"{PATCH_PATH}.combine_non_res_with_and_without_dormancy_models")
    @patch(f"{PATCH_PATH}.enrich_with_model_predictions")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        enrich_with_model_predictions_mock: Mock,
        combine_non_res_with_and_without_dormancy_models_mcok: Mock,
        model_imputation: Mock,
        coalesce_with_source_labels_mock: Mock,
        set_min_value_mock: Mock,
        estimate_non_res_capacity_tracker_filled_posts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        coalesce_with_source_labels_mock.return_value = (Mock(), Mock())

        job.main(
            self.TEST_BUCKET_NAME,
            self.TEST_IMPUTED_IND_CQC_DATA_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            source=ANY,
            selected_columns=job.ind_cqc_columns,
        )
        self.assertEqual(enrich_with_model_predictions_mock.call_count, 3)
        combine_non_res_with_and_without_dormancy_models_mcok.assert_called_once()
        self.assertEqual(model_imputation.call_count, 3)
        coalesce_with_source_labels_mock.assert_called_once()
        set_min_value_mock.assert_called_once_with(
            ANY, job.IndCQC.estimate_filled_posts, 1.0
        )
        estimate_non_res_capacity_tracker_filled_posts_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
        )


class EstimateFilledPostsCastExprTests(unittest.TestCase):
    def test_casts_estimate_filled_posts_to_float32(self):
        lf = pl.LazyFrame(
            {IndCQC.estimate_filled_posts: [1.0]},
            schema={IndCQC.estimate_filled_posts: pl.Float64},
        )
        result_lf = lf.with_columns(job.estimate_filled_posts_cast_expr())
        self.assertEqual(
            result_lf.collect_schema()[IndCQC.estimate_filled_posts], pl.Float32
        )


class EstimateFilledPostsSourceCastExprTests(unittest.TestCase):
    def test_casts_estimate_filled_posts_source_to_enum(self):
        lf = pl.LazyFrame(
            {
                IndCQC.estimate_filled_posts_source: [
                    EstimateFilledPostsSource.ascwds_pir_merged
                ]
            }
        )
        result_lf = lf.with_columns(job.estimate_filled_posts_source_cast_expr())
        self.assertEqual(
            result_lf.collect_schema()[IndCQC.estimate_filled_posts_source],
            CatColType.EstimatesFilledPostSourceEnumType,
        )
