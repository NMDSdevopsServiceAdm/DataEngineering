import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_pir.utils.null_people_directly_employed_outliers as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    NullPeopleDirectlyEmployedData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    NullPeopleDirectlyEmployedSchema as Schemas,
)
from utils import utils
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
    NullPeopleDirectlyEmployedTemporaryColumns as NullPIRTemp,
)

PATCH_PATH: str = (
    "projects._01_ingest.cqc_pir.utils.null_people_directly_employed_outliers"
)


class NullPeopleDirectlyEmployedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()


class MainTests(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.null_people_directly_employed_outliers_rows,
            Schemas.null_people_directly_employed_outliers_schema,
        )
        self.returned_df = job.null_people_directly_employed_outliers(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            [], Schemas.expected_null_people_directly_employed_outliers_schema
        )

    @patch(f"{PATCH_PATH}.null_outliers")
    @patch(f"{PATCH_PATH}.null_large_single_submission_locations")
    def test_main_calls_functions(
        self,
        null_large_single_submission_locations_mock: Mock,
        null_outliers_mock: Mock,
    ):
        job.null_people_directly_employed_outliers(self.test_df)

        null_large_single_submission_locations_mock.assert_called_once()
        null_outliers_mock.assert_called_once()

    def test_main_adds_cleaned_column(self):
        self.assertIn(
            PIRCleanCols.pir_people_directly_employed_cleaned, self.returned_df.columns
        )
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_main_returns_original_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())


class NullLargeSingleSubmissionLocationsTests(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.null_large_single_submission_locations_rows,
            Schemas.null_large_single_submission_locations_schema,
        )
        self.returned_df = job.null_large_single_submission_locations(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_large_single_submission_locations_rows,
            Schemas.null_large_single_submission_locations_schema,
        )

        self.returned_data = self.returned_df.sort(
            PIRCleanCols.location_id, PIRCleanCols.cqc_pir_import_date
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_null_large_single_submission_locations_returns_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)

    def test_null_large_single_submission_locations_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)


class NullOutliersTests(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        self.data_to_filter = 0.5
        self.test_df = self.spark.createDataFrame(
            Data.null_outliers_rows,
            Schemas.null_outliers_schema,
        )

    @patch(f"{PATCH_PATH}.apply_removal_flag")
    @patch(f"{PATCH_PATH}.flag_outliers")
    @patch(f"{PATCH_PATH}.compute_median_absolute_deviation_stats")
    @patch(f"{PATCH_PATH}.compute_dispersion_stats")
    def test_null_outliers_calls_functions(
        self,
        compute_dispersion_stats_mock: Mock,
        compute_median_absolute_deviation_stats_mock: Mock,
        flag_outliers_mock: Mock,
        apply_removal_flag_mock: Mock,
    ):
        job.null_outliers(self.test_df, self.data_to_filter)

        compute_dispersion_stats_mock.assert_called_once()
        compute_median_absolute_deviation_stats_mock.assert_called_once()
        flag_outliers_mock.assert_called_once()
        apply_removal_flag_mock.assert_called_once()


class ComputeDispersionStatsTests(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.null_outliers_rows, Schemas.null_outliers_schema
        )
        self.returned_df = job.compute_dispersion_stats(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_compute_dispersion_stats_rows,
            Schemas.expected_compute_dispersion_stats_schema,
        )

    def test_compute_dispersion_stats_returns_expected_columns(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_compute_dispersion_stats_returns_expected_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()

        for row in range(len(expected_data)):
            for col in self.expected_df.columns:
                self.assertAlmostEqual(
                    returned_data[row][col], expected_data[row][col], places=3
                )


class ComputeMedianAbsoluteDeviationStatsTests(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.null_outliers_rows,
            Schemas.null_outliers_schema,
        )
        self.returned_df = job.compute_median_absolute_deviation_stats(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_compute_median_absolute_deviation_stats_rows,
            Schemas.expected_compute_median_absolute_deviation_stats_schema,
        )

    def test_compute_median_absolute_deviation_stats_returns_expected_columns(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_compute_median_absolute_deviation_stats_returns_expected_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()

        for row in range(len(expected_data)):
            self.assertAlmostEqual(
                returned_data[row][NullPIRTemp.median_absolute_deviation_value],
                expected_data[row][NullPIRTemp.median_absolute_deviation_value],
                places=3,
            )


class FlagOutliers(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        test_dispersion_df = self.spark.createDataFrame(
            Data.expected_compute_dispersion_stats_rows,
            Schemas.expected_compute_dispersion_stats_schema,
        )
        test_median_absolute_deviation_df = self.spark.createDataFrame(
            Data.expected_compute_median_absolute_deviation_stats_rows,
            Schemas.expected_compute_median_absolute_deviation_stats_schema,
        )
        self.returned_df = job.flag_outliers(
            test_dispersion_df,
            test_median_absolute_deviation_df,
            Data.test_flag_outliers_percentile_threshold,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_flag_outliers_rows, Schemas.expected_flag_outliers_schema
        )

    def test_flag_outliers_returns_expected_columns(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_flag_outliers_returns_expected_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class ApplyRemovalFlag(NullPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        test_to_clean_df = self.spark.createDataFrame(
            Data.null_outliers_rows,
            Schemas.null_outliers_schema,
        )
        test_with_outlier_df = self.spark.createDataFrame(
            Data.expected_flag_outliers_rows,
            Schemas.expected_flag_outliers_schema,
        )
        self.returned_df = job.apply_removal_flag(
            test_to_clean_df,
            test_with_outlier_df,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_apply_removal_flag_rows,
            Schemas.expected_apply_removal_flag_schema,
        )

    def test_apply_removal_flag_returns_expected_columns(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_apply_removal_flag_returns_expected_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()

        self.assertEqual(returned_data, expected_data)
