import unittest

from pyspark.sql import WindowSpec

from jobs.diagnostics_on_known_filled_posts import (
    absolute_value_cutoff,
    standardised_value_cutoff,
    percentage_value_cutoff,
)
import utils.diagnostics_utils.diagnostics_utils as job
from tests.test_file_schemas import DiagnosticsUtilsSchemas as Schemas
from tests.test_file_data import DiagnosticsUtilsData as Data
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class DiagnosticsUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class FilterToKnownValuesTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_column = IndCQC.ascwds_filled_posts_dedup_clean
        self.test_df = self.spark.createDataFrame(
            Data.filter_to_known_values_rows, Schemas.filter_to_known_values_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_filter_to_known_values_rows,
            Schemas.filter_to_known_values_schema,
        )
        self.returned_df = job.filter_to_known_values(self.test_df, self.test_column)

    def test_filter_to_known_values_removes_null_values_from_specified_column(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_filter_to_known_values_does_not_remove_any_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_filter_to_known_values_has_expected_row_count(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())


class CreateListOfModelsTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.expected_list_of_models = Data.expected_list_of_models
        self.returned_list_of_models = job.create_list_of_models()

    def test_create_list_of_models_creates_list_using_estimate_source_column(self):
        self.assertEqual(
            sorted(self.expected_list_of_models), sorted(self.returned_list_of_models)
        )


class RestructureDataframeToColumnWiseTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.restructure_dataframe_rows, Schemas.restructure_dataframe_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_restructure_dataframe_rows,
            Schemas.expected_restructure_dataframe_schema,
        )
        self.list_of_models = Data.list_of_models
        self.returned_df = job.restructure_dataframe_to_column_wise(
            self.test_df, IndCQC.ascwds_filled_posts_dedup_clean, self.list_of_models
        )

    def test_restructure_dataframe_to_column_wise_has_correct_values(self):
        returned_data = self.returned_df.sort(IndCQC.estimate_source).collect()
        expected_data = self.expected_df.sort(IndCQC.estimate_source).collect()
        self.assertEqual(returned_data, expected_data)


class CreateWindowForModelAndServiceSplitsTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_window_for_model_and_service_splits_returns_a_window(self):
        returned_win = job.create_window_for_model_and_service_splits()
        self.assertEqual(type(returned_win), WindowSpec)


class CalculateDistributionMetricsTests(DiagnosticsUtilsTests):
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
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.distribution_standard_deviation],
                expected_data[i][IndCQC.distribution_standard_deviation],
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
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.distribution_standard_deviation],
                expected_data[i][IndCQC.distribution_standard_deviation],
                places=6,
            )


class CalculateResidualsTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_residual_adds_column_with_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_residuals_rows,
            Schemas.calculate_residuals_schema,
        )
        returned_df = job.calculate_residual(
            test_df, IndCQC.ascwds_filled_posts_dedup_clean
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_rows,
            Schemas.expected_calculate_residual_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_calculate_absolute_residual_adds_column_with_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_rows,
            Schemas.expected_calculate_residual_schema,
        )
        returned_df = job.calculate_absolute_residual(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_absolute_residual_rows,
            Schemas.expected_calculate_absolute_residual_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_calculate_percentage_residual_adds_column_with_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_residuals_rows,
            Schemas.calculate_residuals_schema,
        )
        returned_df = job.calculate_percentage_residual(
            test_df, IndCQC.ascwds_filled_posts_dedup_clean
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_percentage_residual_rows,
            Schemas.expected_calculate_percentage_residual_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_calculate_standardised_residual_adds_column_with_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.expected_calculate_residual_rows,
            Schemas.expected_calculate_residual_schema,
        )
        returned_df = job.calculate_standardised_residual(
            test_df, IndCQC.ascwds_filled_posts_dedup_clean
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_standardised_residual_rows,
            Schemas.expected_calculate_standardised_residual_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.standardised_residual],
                expected_data[i][IndCQC.standardised_residual],
                places=6,
            )

    def test_calculate_residuals_adds_columns_with_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_residuals_rows,
            Schemas.calculate_residuals_schema,
        )
        returned_df = job.calculate_residuals(
            test_df, IndCQC.ascwds_filled_posts_dedup_clean
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_residuals_rows,
            Schemas.expected_calculate_residuals_schema,
        )
        returned_sr = returned_df.sort(IndCQC.location_id).collect()
        expected_sr = expected_df.sort(IndCQC.location_id).collect()

        for i in range(len(returned_sr)):
            self.assertAlmostEqual(
                returned_sr[i][IndCQC.standardised_residual],
                expected_sr[i][IndCQC.standardised_residual],
                places=6,
            )

        returned_data = (
            returned_df.drop(IndCQC.standardised_residual)
            .sort(IndCQC.location_id)
            .collect()
        )
        expected_data = (
            expected_df.drop(IndCQC.standardised_residual)
            .sort(IndCQC.location_id)
            .collect()
        )
        self.assertEqual(returned_data, expected_data)


class CalculateAggregateResidualsTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.window = job.create_window_for_model_and_service_splits()
        self.test_df = self.spark.createDataFrame(
            Data.calculate_aggregate_residuals_rows,
            Schemas.calculate_aggregate_residuals_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_aggregate_residuals_rows,
            Schemas.expected_calculate_aggregate_residuals_schema,
        )
        self.expected_data = self.expected_df.collect()

    def test_aggregate_residuals_raises_error_when_function_is_not_permitted(
        self,
    ):
        with self.assertRaises(ValueError) as context:
            job.aggregate_residuals(
                self.test_df,
                self.window,
                IndCQC.average_absolute_residual,
                IndCQC.absolute_residual,
                function="other",
            )

        self.assertTrue(
            "Error: The selection function 'other' was not found. Please use 'mean', 'min' or 'max'.",
            "Exception does not contain the correct error message",
        )

    def test_aggregate_residuals_returns_expected_values_when_function_is_mean(
        self,
    ):
        returned_df = job.aggregate_residuals(
            self.test_df,
            self.window,
            IndCQC.average_absolute_residual,
            IndCQC.absolute_residual,
            function="mean",
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.average_absolute_residual],
                self.expected_data[i][IndCQC.average_absolute_residual],
                places=3,
            )

    def test_aggregate_residuals_returns_expected_values_when_function_is_min(
        self,
    ):
        returned_df = job.aggregate_residuals(
            self.test_df,
            self.window,
            IndCQC.min_residual,
            IndCQC.residual,
            function="min",
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.min_residual],
                self.expected_data[i][IndCQC.min_residual],
                places=3,
            )

    def test_aggregate_residuals_returns_expected_values_when_function_is_max(
        self,
    ):
        returned_df = job.aggregate_residuals(
            self.test_df,
            self.window,
            IndCQC.max_residual,
            IndCQC.residual,
            function="max",
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.max_residual],
                self.expected_data[i][IndCQC.max_residual],
                places=3,
            )

    def test_calculate_percentage_of_residuals_within_cutoffs_returns_expected_values(
        self,
    ):
        returned_df = job.calculate_percentage_of_residuals_within_cutoffs(
            self.test_df,
            self.window,
            IndCQC.percentage_of_residuals_within_absolute_value,
            IndCQC.absolute_residual,
            absolute_value_cutoff,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][IndCQC.percentage_of_residuals_within_absolute_value],
                self.expected_data[i][
                    IndCQC.percentage_of_residuals_within_absolute_value
                ],
                places=3,
            )

    def test_calculate_percentage_of_residuals_within_absolute_or_percentage_cutoffs_returns_expected_values(
        self,
    ):
        returned_df = (
            job.calculate_percentage_of_residuals_within_absolute_or_percentage_cutoffs(
                self.test_df,
                self.window,
                absolute_value_cutoff,
                percentage_value_cutoff,
            )
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][
                    IndCQC.percentage_of_residuals_within_absolute_or_percentage_values
                ],
                self.expected_data[i][
                    IndCQC.percentage_of_residuals_within_absolute_or_percentage_values
                ],
                places=3,
            )

    def test_calculate_aggregate_residuals_returns_expected_columns(self):
        returned_df = job.calculate_aggregate_residuals(
            self.test_df,
            self.window,
            absolute_value_cutoff,
            percentage_value_cutoff,
            standardised_value_cutoff,
        )
        self.assertEqual(returned_df.columns, self.expected_df.columns)

    def test_calculate_aggregate_residuals_returns_expected_row_count(self):
        returned_df = job.calculate_aggregate_residuals(
            self.test_df,
            self.window,
            absolute_value_cutoff,
            percentage_value_cutoff,
            standardised_value_cutoff,
        )
        self.assertEqual(returned_df.count(), self.expected_df.count())


class CreateSummaryDataframeTests(DiagnosticsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.create_summary_dataframe_rows, Schemas.create_summary_dataframe_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_summary_dataframe_rows,
            Schemas.expected_create_summary_dataframe_schema,
        )
        self.returned_df = job.create_summary_diagnostics_table(self.test_df)

    def test_create_summary_diagnostics_table_returns_correct_data(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
