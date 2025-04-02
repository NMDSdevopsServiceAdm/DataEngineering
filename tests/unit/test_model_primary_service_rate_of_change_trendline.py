import unittest
import warnings

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import utils.estimate_filled_posts.models.primary_service_rate_of_change_trendline as job
from tests.test_file_data import ModelPrimaryServiceRateOfChangeTrendlineData as Data
from tests.test_file_schemas import (
    ModelPrimaryServiceRateOfChangeTrendlineSchemas as Schemas,
)


class ModelPrimaryServiceRateOfChangeTrendlineTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelPrimaryServiceRateOfChangeTrendlineTests):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days: int = 3
        self.test_df = self.spark.createDataFrame(
            Data.primary_service_rate_of_change_trendline_rows,
            Schemas.primary_service_rate_of_change_trendline_schema,
        )
        self.returned_df = job.model_primary_service_rate_of_change_trendline(
            self.test_df,
            IndCqc.combined_ratio_and_filled_posts,
            self.number_of_days,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rate_of_change_trendline_rows,
            Schemas.expected_primary_service_rate_of_change_trendline_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.test_df.count(), self.returned_df.count())

    def test_returned_rate_of_change_trendline_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                self.expected_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                3,
                f"Returned row {i} does not match expected",
            )

    def test_primary_service_rate_of_change_trendline_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )


class DeduplicateDataframeTests(ModelPrimaryServiceRateOfChangeTrendlineTests):
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


class CalculateRateOfChangeTrendlineTests(
    ModelPrimaryServiceRateOfChangeTrendlineTests
):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_rate_of_change_trendline_rows,
            Schemas.calculate_rate_of_change_trendline_schema,
        )
        self.returned_df = job.calculate_rate_of_change_trendline(
            test_df,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rate_of_change_trendline_rows,
            Schemas.expected_calculate_rate_of_change_trendline_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_rate_of_change_trendline_returns_correct_values_in_rate_of_change_trendline_model_column(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                self.expected_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                2,
                f"Returned row {i} does not match expected",
            )
