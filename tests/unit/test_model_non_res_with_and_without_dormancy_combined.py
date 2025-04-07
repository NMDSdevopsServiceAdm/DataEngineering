import unittest
from unittest.mock import patch, Mock

from utils import utils
import utils.estimate_filled_posts.models.non_res_with_and_without_dormancy_combined as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    NonResWithAndWithoutDormancyCombinedColumns as NRModel_TempCol,
)
from tests.test_file_data import ModelNonResWithAndWithoutDormancyCombinedRows as Data
from tests.test_file_schemas import (
    ModelNonResWithAndWithoutDormancyCombinedSchemas as Schemas,
)


class ModelNonResWithAndWithoutDormancyCombinedTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class MainTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        self.estimated_posts_df = self.spark.createDataFrame(
            Data.estimated_posts_rows, Schemas.estimated_posts_schema
        )

    @patch("utils.utils.select_rows_with_value")
    def test_models_runs(self, select_rows_with_value_mock: Mock):
        returned_df = job.combine_non_res_with_and_without_dormancy_models(
            self.estimated_posts_df
        )

        select_rows_with_value_mock.assert_called_once()

    # TODO flesh out main tests to usual standard (expected columns/rows/anything else?)


class CalculateAndApplyModelRatioTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_and_apply_model_ratios_rows,
            Schemas.calculate_and_apply_model_ratios_schema,
        )
        self.returned_df = job.calculate_and_apply_model_ratios(self.test_df)

        self.empty_expected_df = self.spark.createDataFrame(
            [], Schemas.expected_calculate_and_apply_model_ratios_schema
        )

    def test_calculate_and_apply_model_ratios_returns_the_original_row_count(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_calculate_and_apply_model_ratios_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.empty_expected_df.columns)
        )


class AverageModelsByRelatedLocationAndTimeRegisteredTests(
    ModelNonResWithAndWithoutDormancyCombinedTests
):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.average_models_by_related_location_and_time_registered_rows,
            Schemas.average_models_by_related_location_and_time_registered_schema,
        )
        self.returned_df = job.average_models_by_related_location_and_time_registered(
            test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_average_models_by_related_location_and_time_registered_rows,
            Schemas.expected_average_models_by_related_location_and_time_registered_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.related_location, IndCqc.time_registered
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_average_models_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_average_models_returns_expected_average_with_dormancy_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.avg_with_dormancy],
                self.expected_data[i][NRModel_TempCol.avg_with_dormancy],
                places=3,
                msg=f"Returned with dormancy average for row {i} does not match expected",
            )

    def test_average_models_returns_expected_average_without_dormancy_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.avg_without_dormancy],
                self.expected_data[i][NRModel_TempCol.avg_without_dormancy],
                places=3,
                msg=f"Returned without dormancy average for row {i} does not match expected",
            )


class CalculateAdjustmentRatiosTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_adjustment_ratios_rows,
            Schemas.calculate_adjustment_ratios_schema,
        )
        self.returned_df = job.calculate_adjustment_ratios(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_adjustment_ratios_rows,
            Schemas.expected_calculate_adjustment_ratios_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.related_location).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_adjustment_ratios_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_calculate_adjustment_ratios_returns_expected_ratios(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.adjustment_ratio],
                self.expected_data[i][NRModel_TempCol.adjustment_ratio],
                places=3,
                msg=f"Returned adjustment ratio for row {i} does not match expected",
            )

    def test_calculate_adjustment_ratios_when_without_dormancy_is_zero_or_null_returns_one(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_adjustment_ratios_when_without_dormancy_is_zero_or_null_returns_one_rows,
            Schemas.calculate_adjustment_ratios_schema,
        )
        returned_df = job.calculate_adjustment_ratios(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_adjustment_ratios_when_without_dormancy_is_zero_or_null_returns_one_rows,
            Schemas.expected_calculate_adjustment_ratios_schema,
        )

        returned_data = returned_df.sort(IndCqc.related_location).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertAlmostEqual(
                returned_data[i][NRModel_TempCol.adjustment_ratio],
                expected_data[i][NRModel_TempCol.adjustment_ratio],
                places=3,
                msg=f"Returned adjustment ratio for row {i} does not match expected",
            )


class ApplyModelRatiosTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.apply_model_ratios_returns_expected_values_when_all_values_known_rows,
            Schemas.apply_model_ratios_schema,
        )
        self.returned_df = job.apply_model_ratios(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_apply_model_ratios_returns_expected_values_when_all_values_known_rows,
            Schemas.expected_apply_model_ratios_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_apply_model_ratios_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_apply_model_ratios_returns_expected_values_when_all_values_known(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.without_dormancy_model_adjusted],
                self.expected_data[i][NRModel_TempCol.without_dormancy_model_adjusted],
                places=3,
                msg=f"Returned values for row {i} does not match expected",
            )

    def test_apply_model_ratios_returns_none_when_none_values_present(self):
        test_df = self.spark.createDataFrame(
            Data.apply_model_ratios_returns_none_when_none_values_present_rows,
            Schemas.apply_model_ratios_schema,
        )
        returned_df = job.apply_model_ratios(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_apply_model_ratios_returns_none_when_none_values_present_rows,
            Schemas.expected_apply_model_ratios_schema,
        )

        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][NRModel_TempCol.without_dormancy_model_adjusted],
                expected_data[i][NRModel_TempCol.without_dormancy_model_adjusted],
                f"Returned values for row {i} does not match expected",
            )


class CalculateAndApplyResidualsTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_and_apply_residuals_rows,
            Schemas.calculate_and_apply_residuals_schema,
        )
        self.returned_df = job.calculate_and_apply_residuals(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_and_apply_residuals_rows,
            Schemas.expected_calculate_and_apply_residuals_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_and_apply_residuals_returns_original_dataframe_row_count(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_calculate_and_apply_residuals_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_calculate_and_apply_residuals_returns_expected_residual_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.residual_at_overlap],
                self.expected_data[i][NRModel_TempCol.residual_at_overlap],
                places=3,
                msg=f"Returned residual for row {i} does not match expected",
            )

    def test_calculate_and_apply_residuals_returns_expected_model_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][
                    NRModel_TempCol.without_dormancy_model_adjusted_and_residual_applied
                ],
                self.expected_data[i][
                    NRModel_TempCol.without_dormancy_model_adjusted_and_residual_applied
                ],
                places=3,
                msg=f"Returned residual for row {i} does not match expected",
            )


class CalculateResidualsTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_residuals_rows,
            Schemas.calculate_residuals_schema,
        )
        self.returned_df = job.calculate_residuals(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_residuals_rows,
            Schemas.expected_calculate_residuals_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_residuals_filters_to_the_expected_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCqc.location_id],
                self.expected_data[i][IndCqc.location_id],
                msg=f"Returned location_id for row {i} does not match expected",
            )

    def test_calculate_residuals_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_calculate_residuals_returns_expected_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.residual_at_overlap],
                self.expected_data[i][NRModel_TempCol.residual_at_overlap],
                places=3,
                msg=f"Returned residual for row {i} does not match expected",
            )


class ApplyResidualsTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.apply_residuals_when_no_null_values_rows,
            Schemas.apply_residuals_schema,
        )
        self.returned_df = job.apply_residuals(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_apply_residuals_when_no_null_values_rows,
            Schemas.expected_apply_residuals_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_apply_residuals_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_apply_residuals_returns_expected_values_when_no_nulls_present(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][
                    NRModel_TempCol.without_dormancy_model_adjusted_and_residual_applied
                ],
                self.expected_data[i][
                    NRModel_TempCol.without_dormancy_model_adjusted_and_residual_applied
                ],
                places=3,
                msg=f"Returned value for row {i} does not match expected",
            )

    def test_apply_residuals_returns_expected_values_when_data_contains_null_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.apply_residuals_when_null_values_present_rows,
            Schemas.apply_residuals_schema,
        )
        returned_df = job.apply_residuals(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_apply_residuals_when_null_values_present_rows,
            Schemas.expected_apply_residuals_schema,
        )

        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.collect()

        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                returned_data[i][
                    NRModel_TempCol.without_dormancy_model_adjusted_and_residual_applied
                ],
                expected_data[i][
                    NRModel_TempCol.without_dormancy_model_adjusted_and_residual_applied
                ],
                places=3,
                msg=f"Returned value for row {i} does not match expected",
            )


class CombineModelPredictionsTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.combine_model_predictions_rows,
            Schemas.combine_model_predictions_schema,
        )
        self.returned_df = job.combine_model_predictions(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_combine_model_predictions_rows,
            Schemas.expected_combine_model_predictions_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_combine_model_predictions_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_combine_model_predictions_returns_expected_prediction_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.prediction],
                self.expected_data[i][IndCqc.prediction],
                places=3,
                msg=f"Returned prediction value for row {i} does not match expected",
            )
