import unittest
from unittest.mock import patch, Mock, ANY
import warnings
from pyspark.ml.evaluation import RegressionEvaluator

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import utils.estimate_filled_posts.model_metrics as job
from tests.test_file_data import ModelMetrics as Data
from tests.test_file_schemas import ModelMetrics as Schemas


class SaveModelMetricsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.evaluator = RegressionEvaluator(
            predictionCol=IndCqc.prediction, labelCol=IndCqc.imputed_filled_post_model
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    # MODEL_SOURCE = "some/model/version/"
    # METRICS_DESTINATION = "some/destination"
    # DEPENDENT_COLUMN_NAME = IndCqc.ascwds_pir_merged

    # partition_keys = [IndCqc.model_name, IndCqc.model_version]

    # @patch("utils.utils.write_to_parquet")
    # def test_main_runs(
    #     self,
    #     write_to_parquet_patch: Mock,
    # ):
    #     df_with_predictions = self.spark.createDataFrame(
    #         Data.ind_cqc_with_predictions_rows, Schemas.ind_cqc_with_predictions_schema
    #     )

    #     job.save_model_metrics(
    #         df_with_predictions,
    #         self.DEPENDENT_COLUMN_NAME,
    #         self.MODEL_SOURCE,
    #         self.METRICS_DESTINATION,
    #     )

    #     write_to_parquet_patch.assert_called_once_with(
    #         ANY,
    #         self.METRICS_DESTINATION,
    #         mode="append",
    #         partitionKeys=self.partition_keys,
    #     )


class GenerateMetricTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generic_metric_returns_float(self):
        r2 = job.generate_metric(
            self.evaluator, self.care_home_predictions_df, IndCqc.r2
        )
        self.assertIsInstance(r2, float)


class CalculateResidualBetweenPredictedAndKnownFilledPostsTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

        self.calculate_residual_non_res_df = self.spark.createDataFrame(
            Data.calculate_residual_non_res_rows, Schemas.calculate_residual_schema
        )
        self.returned_non_res_df = (
            job.calculate_residual_between_predicted_and_known_filled_posts(
                self.calculate_residual_non_res_df
            )
        )
        expected_non_res_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_non_res_rows,
            Schemas.expected_calculate_residual_schema,
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
            Data.calculate_residual_care_home_rows, Schemas.calculate_residual_schema
        )
        returned_care_home_df = (
            job.calculate_residual_between_predicted_and_known_filled_posts(
                self.calculate_residual_care_home_df,
                is_care_home_model=True,
            )
        )
        expected_care_home_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_care_home_rows,
            Schemas.expected_calculate_residual_schema,
        )

        returned_data = returned_care_home_df.sort(IndCqc.location_id).collect()
        expected_data = expected_care_home_df.collect()

        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCqc.residual],
                expected_data[i][IndCqc.residual],
                places=1,
            )


class GenerateProportionOfPredictionsWithinRangeTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_proportion_of_predictions_within_range_returns_expected_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.generate_proportion_of_predictions_within_range_rows,
            Schemas.generate_proportion_of_predictions_within_range_schema,
        )
        returned_proportion = job.generate_proportion_of_predictions_within_range(
            test_df, Data.range_cutoff
        )

        self.assertAlmostEqual(returned_proportion, Data.expected_proportion, places=1)


class StoreModelMetricsTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()


class ModelNameAndVersionFromFilepathTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_returns_correct_model_name_and_version_from_s3_filepath(self):
        model_source = "s3://pipeline-resources/models/model_prediction/1.0.0/run=5/"
        (
            model_name,
            model_version,
            run_number,
        ) = job.get_model_name_and_version_from_s3_filepath(model_source)

        self.assertEqual(model_name, "model_prediction")
        self.assertEqual(model_version, "1.0.0")
        self.assertEqual(run_number, "run=5")
