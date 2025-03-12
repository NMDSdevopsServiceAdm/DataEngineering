import unittest

from pyspark.sql import functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import utils.estimate_filled_posts.models.prediction_rolling_average as job
from tests.test_file_data import ModelPredictionRollingAverageData as Data
from tests.test_file_schemas import ModelPredictionRollingAverageSchemas as Schemas


class ModelPredictionRollingAverageTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class MainTests(ModelPredictionRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 3

        test_df = self.spark.createDataFrame(
            Data.rolling_average_rows,
            Schemas.rolling_average_schema,
        )
        self.returned_df = job.calculate_prediction_rolling_average(
            test_df, number_of_days
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_rolling_average_rows,
            Schemas.expected_rolling_average_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_prediction_rolling_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_prediction_rolling_average_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.prediction_rolling_average],
                self.expected_data[i][IndCqc.prediction_rolling_average],
                2,
                f"Returned row {i} does not match expected",
            )
