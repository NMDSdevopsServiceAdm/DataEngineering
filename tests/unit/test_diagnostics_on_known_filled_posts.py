import unittest
from unittest.mock import patch, Mock, ANY, call

from pyspark.sql import WindowSpec

import jobs.diagnostics_on_known_filled_posts as job
from tests.test_file_schemas import (
    DiagnosticsOnKnownFilledPostsSchemas as Schemas,
)
from tests.test_file_data import (
    DiagnosticsOnKnownFilledPostsData as Data,
)
from utils import utils
from utils.column_values.categorical_column_values import CareHome
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


class DiagnosticsOnKnownFilledPostsTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    DIAGNOSTICS_DESTINATION = "some/other/directory"
    SUMMARY_DIAGNOSTICS_DESTINATION = "another/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.spark = utils.get_spark()
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_filled_posts_rows,
            Schemas.estimate_filled_posts_schema,
        )


class MainTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.read_from_parquet")
    def test_main_runs(self, read_from_parquet_patch: Mock):
        read_from_parquet_patch.return_value = self.estimate_jobs_df

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.DIAGNOSTICS_DESTINATION,
            self.SUMMARY_DIAGNOSTICS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)


class FilterToKnownValuesTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_column = IndCQC.ascwds_filled_posts_clean
        self.test_df = self.spark.createDataFrame(
            Data.filter_to_known_values_rows, Schemas.filter_to_known_values_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_filter_to_known_values_rows,
            Schemas.filter_to_known_values_schema,
        )
        self.returned_df = job.filter_to_known_values(self.test_df, self.test_column)

    @unittest.skip("to do")
    def test_filter_to_known_values_removes_null_values_from_specified_column(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class RestructureDataframeToColumnWiseTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.restructure_dataframe_rows, Schemas.restructure_dataframe_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_restructure_dataframe_rows,
            Schemas.expected_restructure_dataframe_schema,
        )
        self.returned_df = job.restructure_dataframe_to_column_wise(self.test_df)

    def test_restructure_dataframe_to_column_wise_has_correct_values(self):
        returned_data = self.returned_df.sort(IndCQC.estimate_source).collect()
        expected_data = self.expected_df.sort(IndCQC.estimate_source).collect()
        self.assertEqual(returned_data, expected_data)


class CreateWindowForModelAndServiceSplitsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_window_for_model_and_service_splits_returns_a_window(self):
        returned_win = job.create_window_for_model_and_service_splits()
        self.assertEqual(type(returned_win), WindowSpec)


class CalculateDistributionMetricsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()
        self.window = job.create_window_for_model_and_service_splits()

    def test_calculate_mean_over_window_returns_extpected_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_distribution_metrics_rows,
            Schemas.calculate_distribution_metrics_schema,
        )
        returned_df = job.calculate_mean_over_window(test_df, self.window)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_distribution_mean_rows,
            Schemas.expected_calculate_mean_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_calculate_standard_deviation_over_window_returns_extpected_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_distribution_metrics_rows,
            Schemas.calculate_distribution_metrics_schema,
        )
        returned_df = job.calculate_standard_deviation_over_window(test_df, self.window)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_distribution_standard_deviation_rows,
            Schemas.expected_calculate_standard_deviation_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertAlmostEqual(
            returned_data[0][IndCQC.distribution_standard_deviation],
            expected_data[0][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[1][IndCQC.distribution_standard_deviation],
            expected_data[1][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[2][IndCQC.distribution_standard_deviation],
            expected_data[2][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[3][IndCQC.distribution_standard_deviation],
            expected_data[3][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[4][IndCQC.distribution_standard_deviation],
            expected_data[4][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[5][IndCQC.distribution_standard_deviation],
            expected_data[5][IndCQC.distribution_standard_deviation],
            places=6,
        )

    def test_calculate_kurtosis_over_window_returns_extpected_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_distribution_metrics_rows,
            Schemas.calculate_distribution_metrics_schema,
        )
        returned_df = job.calculate_kurtosis_over_window(test_df, self.window)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_distribution_kurtosis_rows,
            Schemas.expected_calculate_kurtosis_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_calculate_skewness_over_window_returns_extpected_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_distribution_metrics_rows,
            Schemas.calculate_distribution_metrics_schema,
        )
        returned_df = job.calculate_skewness_over_window(test_df, self.window)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_distribution_skewness_rows,
            Schemas.expected_calculate_skewness_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_calculate_distribution_metrics_returns_extpected_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_distribution_metrics_rows,
            Schemas.calculate_distribution_metrics_schema,
        )
        returned_df = job.calculate_distribution_metrics(test_df, self.window)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_distribution_metrics_rows,
            Schemas.expected_calculate_distribution_metrics_schema,
        )
        returned_data = (
            returned_df.select(
                IndCQC.location_id, IndCQC.distribution_standard_deviation
            )
            .sort(IndCQC.location_id)
            .collect()
        )
        expected_data = (
            expected_df.select(
                IndCQC.location_id, IndCQC.distribution_standard_deviation
            )
            .sort(IndCQC.location_id)
            .collect()
        )
        returned_non_sd_data = (
            returned_df.select(
                IndCQC.location_id,
                IndCQC.distribution_mean,
                IndCQC.distribution_kurtosis,
                IndCQC.distribution_skewness,
            )
            .sort(IndCQC.location_id)
            .collect()
        )
        expected_non_sd_data = (
            expected_df.select(
                IndCQC.location_id,
                IndCQC.distribution_mean,
                IndCQC.distribution_kurtosis,
                IndCQC.distribution_skewness,
            )
            .sort(IndCQC.location_id)
            .collect()
        )
        self.assertEqual(returned_non_sd_data, expected_non_sd_data)
        self.assertAlmostEqual(
            returned_data[0][IndCQC.distribution_standard_deviation],
            expected_data[0][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[1][IndCQC.distribution_standard_deviation],
            expected_data[1][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[2][IndCQC.distribution_standard_deviation],
            expected_data[2][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[3][IndCQC.distribution_standard_deviation],
            expected_data[3][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[4][IndCQC.distribution_standard_deviation],
            expected_data[4][IndCQC.distribution_standard_deviation],
            places=6,
        )
        self.assertAlmostEqual(
            returned_data[5][IndCQC.distribution_standard_deviation],
            expected_data[5][IndCQC.distribution_standard_deviation],
            places=6,
        )


class CalculateResidualsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


class CalculateAggregateResidualsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
