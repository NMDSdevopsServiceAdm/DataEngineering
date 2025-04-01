import unittest
import warnings

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
import utils.estimate_filled_posts.models.primary_service_rolling_rate_of_change as job
from tests.test_file_data import ModelPrimaryServiceRollingAverage as Data
from tests.test_file_schemas import ModelPrimaryServiceRollingAverage as Schemas


class ModelPrimaryServiceRollingAverageTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class DeduplicateDataframeTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.deduplicate_dataframe_rows,
            Schemas.deduplicate_dataframe_schema,
        )
        self.returned_df = job.deduplicate_dataframe(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_deduplicate_dataframe_rows,
            Schemas.expected_deduplicate_dataframe_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returned_deduplicated_dataframe_rows_match_expected(self):
        self.assertEqual(self.returned_data, self.expected_data)


class CalculateCumulativeRateOfChangeTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.cumulative_rate_of_change_rows,
            Schemas.cumulative_rate_of_change_schema,
        )
        self.returned_df = job.calculate_cumulative_rate_of_change(
            test_df, IndCqc.rolling_rate_of_change_model
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_cumulative_rate_of_change_rows,
            Schemas.expected_cumulative_rate_of_change_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_cumulative_rate_of_change_returns_correct_values_in_rolling_rate_of_change_model_column(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.rolling_rate_of_change_model],
                self.expected_data[i][IndCqc.rolling_rate_of_change_model],
                2,
                f"Returned row {i} does not match expected",
            )
