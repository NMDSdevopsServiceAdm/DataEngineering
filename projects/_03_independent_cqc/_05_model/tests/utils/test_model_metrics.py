import unittest
from unittest.mock import patch, MagicMock, Mock, ANY
import warnings
from pyspark.ml.evaluation import RegressionEvaluator

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import projects._03_independent_cqc._05_model.utils.model_metrics as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelMetrics as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelMetrics as Schemas,
)

PATCH_PATH: str = "projects._03_independent_cqc._05_model.utils.model_metrics"


class SaveModelMetricsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.mock_model = MagicMock()
        self.test_df = self.spark.createDataFrame(
            Data.model_metrics_rows, Schemas.model_metrics_schema
        )
        self.metrics_df = self.spark.createDataFrame(
            Data.expected_combined_metrics_rows,
            Schemas.expected_combined_metrics_schema,
        )
        self.dependent_variable: str = IndCqc.imputed_filled_post_model
        self.branch_name: str = "test_branch"
        self.model_name: str = "test_model"
        self.model_version: str = "1.0.0"
        self.model_run_number: int = 3
        self.partition_keys = [
            IndCqc.model_name,
            IndCqc.model_version,
            IndCqc.run_number,
        ]
        self.metrics_path: str = "some/path"

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.generate_metric")
    @patch(f"{PATCH_PATH}.calculate_residual_between_predicted_and_known_filled_posts")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.generate_model_metrics_s3_path")
    def test_main_runs(
        self,
        generate_model_metrics_s3_path_mock: Mock,
        read_from_parquet_mock: Mock,
        calculate_residual_between_predicted_and_known_filled_posts_mock: Mock,
        generate_metric_mock: Mock,
    ):
        generate_model_metrics_s3_path_mock.return_value = self.metrics_path
        generate_metric_mock.return_value = 0.5

        job.save_model_metrics(
            self.mock_model,
            self.test_df,
            self.dependent_variable,
            self.branch_name,
            self.model_name,
            self.model_version,
            self.model_run_number,
        )

        generate_model_metrics_s3_path_mock.assert_called_once()
        read_from_parquet_mock.assert_called_once_with(self.metrics_path),
        calculate_residual_between_predicted_and_known_filled_posts_mock.assert_called_once()
        self.assertEqual(generate_metric_mock.call_count, 2)


class GenerateModelMetricsS3PathTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_model_s3_path_returns_expected_path(self):
        returned_path = job.generate_model_metrics_s3_path(
            self.branch_name, self.model_name, self.model_version
        )
        expected_path = "s3://sfc-test_branch-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_metrics/model_name=test_model/model_version=1.0.0/"

        self.assertEqual(returned_path, expected_path)


class CalculateResidualBetweenPredictedAndKnownFilledPostsTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

        self.calculate_residual_non_res_df = self.spark.createDataFrame(
            Data.calculate_residual_non_res_rows,
            Schemas.calculate_residual_non_res_schema,
        )
        self.returned_non_res_df = (
            job.calculate_residual_between_predicted_and_known_filled_posts(
                self.calculate_residual_non_res_df, IndCqc.non_res_with_dormancy_model
            )
        )
        expected_non_res_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_non_res_rows,
            Schemas.expected_calculate_residual_non_res_schema,
        )

        self.returned_non_res_data = self.returned_non_res_df.sort(
            IndCqc.location_id
        ).collect()
        self.expected_non_res_data = expected_non_res_df.collect()

    def test_calculate_residual_returns_residual_column(self):
        self.assertIn(IndCqc.residual, self.returned_non_res_df.columns)

    def test_calculate_residual_when_not_a_care_home(self):
        for i in range(len(self.returned_non_res_data)):
            self.assertAlmostEqual(
                self.returned_non_res_data[i][IndCqc.residual],
                self.expected_non_res_data[i][IndCqc.residual],
                places=1,
            )

    def test_calculate_residual_when_is_a_care_home(self):
        self.calculate_residual_care_home_df = self.spark.createDataFrame(
            Data.calculate_residual_care_home_rows,
            Schemas.calculate_residual_care_home_schema,
        )
        returned_care_home_df = (
            job.calculate_residual_between_predicted_and_known_filled_posts(
                self.calculate_residual_care_home_df,
                IndCqc.care_home_model,
            )
        )
        expected_care_home_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_care_home_rows,
            Schemas.expected_calculate_residual_care_home_schema,
        )

        returned_data = returned_care_home_df.sort(IndCqc.location_id).collect()
        expected_data = expected_care_home_df.collect()

        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCqc.residual],
                expected_data[i][IndCqc.residual],
                places=1,
            )


class GenerateMetricTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

        evaluator = RegressionEvaluator(
            predictionCol=IndCqc.prediction, labelCol=IndCqc.imputed_filled_post_model
        )
        generate_metric_df = self.spark.createDataFrame(
            Data.generate_metric_rows, Schemas.generate_metric_schema
        )
        self.r2 = job.generate_metric(evaluator, generate_metric_df, IndCqc.r2)

    def test_generic_metric_returns_float(self):
        self.assertIsInstance(self.r2, float)
