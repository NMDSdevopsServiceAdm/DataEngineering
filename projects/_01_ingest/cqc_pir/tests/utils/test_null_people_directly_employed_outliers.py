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

    @patch(f"{PATCH_PATH}.null_large_single_submission_locations")
    def test_main_calls_functions(
        self,
        null_large_single_submission_locations_mock: Mock,
    ):
        job.null_people_directly_employed_outliers(self.test_df)

        null_large_single_submission_locations_mock.assert_called_once()

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
