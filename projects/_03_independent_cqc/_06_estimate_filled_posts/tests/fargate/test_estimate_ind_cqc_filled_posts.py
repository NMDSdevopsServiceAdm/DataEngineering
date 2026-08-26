import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.estimate_ind_cqc_filled_posts as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EstimateFilledPostsSource

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.estimate_ind_cqc_filled_posts"

FLOAT32_COLUMNS = [
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.care_home_model,
    IndCQC.non_res_combined_model,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.posts_rolling_average_model,
    IndCQC.estimate_filled_posts,
]


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


class MainDtypeCastTests(unittest.TestCase):
    TEST_BUCKET_NAME = "some/bucket/name"
    TEST_IMPUTED_IND_CQC_DATA_SOURCE = "some/s3/uri"
    TEST_DESTINATION = "some/other/s3/uri"

    @staticmethod
    def add_float64_column(name: str):
        def side_effect(lf, *_args, **_kwargs):
            return lf.with_columns(pl.lit(1.0).cast(pl.Float64).alias(name))

        return side_effect

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.estimate_non_res_capacity_tracker_filled_posts")
    @patch(f"{PATCH_PATH}.set_min_value")
    @patch(f"{PATCH_PATH}.utils.coalesce_with_source_labels")
    @patch(f"{PATCH_PATH}.model_imputation")
    @patch(f"{PATCH_PATH}.combine_non_res_with_and_without_dormancy_models")
    @patch(f"{PATCH_PATH}.enrich_with_model_predictions")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_casts_estimate_filled_posts_source_to_enum_and_named_columns_to_float32(
        self,
        scan_parquet_mock: Mock,
        enrich_with_model_predictions_mock: Mock,
        combine_non_res_with_and_without_dormancy_models_mock: Mock,
        model_imputation_mock: Mock,
        coalesce_with_source_labels_mock: Mock,
        set_min_value_mock: Mock,
        estimate_non_res_capacity_tracker_filled_posts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                IndCQC.ascwds_filled_posts_dedup_clean: [1.0],
                IndCQC.ascwds_pir_merged: [1.0],
                IndCQC.posts_rolling_average_model: [1.0],
            }
        )
        enrich_with_model_predictions_mock.side_effect = (
            lambda lf, bucket_name, model_name: lf.with_columns(
                pl.lit(1.0).cast(pl.Float64).alias(model_name)
            )
        )
        combine_non_res_with_and_without_dormancy_models_mock.side_effect = (
            self.add_float64_column(IndCQC.non_res_combined_model)
        )
        model_imputation_mock.side_effect = lambda lf, _col, _model_col, imputed_column_name, **_kwargs: lf.with_columns(
            pl.lit(1.0).cast(pl.Float64).alias(imputed_column_name)
        )
        coalesce_with_source_labels_mock.return_value = (
            pl.lit(1.0).cast(pl.Float64).alias(IndCQC.estimate_filled_posts),
            pl.lit(EstimateFilledPostsSource.ascwds_pir_merged).alias(
                IndCQC.estimate_filled_posts_source
            ),
        )
        set_min_value_mock.side_effect = lambda lf, *_args, **_kwargs: lf
        estimate_non_res_capacity_tracker_filled_posts_mock.side_effect = lambda lf: lf

        job.main(
            self.TEST_BUCKET_NAME,
            self.TEST_IMPUTED_IND_CQC_DATA_SOURCE,
            self.TEST_DESTINATION,
        )

        # cast at creation, before model_imputation starts consuming these columns
        model_imputation_input_schema = (
            model_imputation_mock.call_args_list[0].args[0].collect_schema()
        )
        self.assertEqual(
            model_imputation_input_schema[IndCQC.care_home_model], pl.Float32
        )
        self.assertEqual(
            model_imputation_input_schema[IndCQC.non_res_combined_model], pl.Float32
        )

        sunk_lf = sink_to_parquet_mock.call_args_list[0].args[0]
        sunk_schema = sunk_lf.collect_schema()

        for column in FLOAT32_COLUMNS:
            self.assertEqual(sunk_schema[column], pl.Float32)
        self.assertEqual(
            sunk_schema[IndCQC.estimate_filled_posts_source],
            CatColType.EstimatesFilledPostSourceEnumType,
        )
