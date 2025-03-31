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

        self.test_df = self.spark.createDataFrame(
            Data.estimated_posts_rows, Schemas.estimated_posts_schema
        )


class MainTests(ModelNonResWithAndWithoutDormancyCombinedTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.select_rows_with_value")
    def test_models_runs(self, select_rows_with_value_mock: Mock):
        returned_df = job.combine_non_res_with_and_without_dormancy_models(self.test_df)

        select_rows_with_value_mock.assert_called_once()

    # TODO flesh out main tests to usual standard (expected columns/rows/anything else?)


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
            Data.calculate_adjustment_ratios_schema,
            Schemas.calculate_adjustment_ratios_schema,
        )
        self.returned_df = job.calculate_adjustment_ratios(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_adjustment_ratios_schema,
            Schemas.expected_calculate_adjustment_ratios_schema,
        )

        self.returned_data = self.returned_df.sort(IndCqc.related_location).collect()
        self.expected_data = self.expected_df.collect()

    def test_average_models_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_average_models_returns_expected_ratios(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][NRModel_TempCol.adjustment_ratio],
                self.expected_data[i][NRModel_TempCol.adjustment_ratio],
                places=3,
                msg=f"Returned adjustment ratio for row {i} does not match expected",
            )
