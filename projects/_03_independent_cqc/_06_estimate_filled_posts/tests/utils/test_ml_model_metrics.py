import unittest
from unittest.mock import patch, Mock, ANY
import warnings

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)

import projects._03_independent_cqc._06_estimate_filled_posts.utils.ml_model_metrics as job
from tests.test_file_data import MLModelMetrics as Data
from tests.test_file_schemas import MLModelMetrics as Schemas


PATCH_PATH = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.ml_model_metrics"
)


class TestGenerateMLModelMetrics(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(TestGenerateMLModelMetrics):
    def setUp(self) -> None:
        super().setUp()

    MODEL_SOURCE = "some/model/version/"
    METRICS_DESTINATION = "some/destination"
    DEPENDENT_COLUMN_NAME = IndCqc.ascwds_pir_merged

    partition_keys = [IndCqc.model_name, IndCqc.model_version]

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    def test_main_runs(
        self,
        write_to_parquet_patch: Mock,
    ):
        df_with_predictions = self.spark.createDataFrame(
            Data.ind_cqc_with_predictions_rows, Schemas.ind_cqc_with_predictions_schema
        )

        job.save_model_metrics(
            df_with_predictions,
            self.DEPENDENT_COLUMN_NAME,
            self.MODEL_SOURCE,
            self.METRICS_DESTINATION,
        )

        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.METRICS_DESTINATION,
            mode="append",
            partitionKeys=self.partition_keys,
        )


class GenerateR2MetricTests(TestGenerateMLModelMetrics):
    def setUp(self) -> None:
        super().setUp()

    def test_generates_expected_r2_metric(self):
        df = self.spark.createDataFrame(Data.r2_metric_rows, Schemas.r2_metric_schema)
        r2 = job.generate_r2_metric(df, IndCqc.prediction, IndCqc.ascwds_pir_merged)

        self.assertAlmostEqual(r2, 0.93, places=2)


class ModelNameAndVersionFromFilepathTests(TestGenerateMLModelMetrics):
    def setUp(self) -> None:
        super().setUp()

    def test_returns_correct_model_name_and_version_from_s3_filepath(self):
        model_source = "s3://pipeline-resources/models/model_prediction/1.0.0/"
        model_name, model_version = job.get_model_name_and_version_from_s3_filepath(
            model_source
        )

        self.assertEqual(model_name, "model_prediction")
        self.assertEqual(model_version, "1.0.0")


class GetPredictionsWithKnownDependentVariableTests(TestGenerateMLModelMetrics):
    def setUp(self) -> None:
        super().setUp()

    def test_returned_data_matches_expected_data(self):
        input_df = self.spark.createDataFrame(
            Data.predictions_rows, Schemas.predictions_schema
        )
        returned_df = job.get_predictions_with_known_dependent_variable_df(
            input_df, IndCqc.ascwds_pir_merged
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_predictions_with_dependent_rows, Schemas.predictions_schema
        )

        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.sort(IndCqc.location_id).collect()
        self.assertEqual(returned_data, expected_data)
