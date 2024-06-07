import unittest
from unittest.mock import patch, ANY
from utils import utils, cleaning_utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from tests.test_file_schemas import (
    CQCPIRSchema as Schemas,
    CQCPIRCleanSchema as CleanSchemas,
)
from tests.test_file_data import CQCPirCleanedData as CleanedData, CQCpirData as Data

import jobs.clean_cqc_pir_data as job


class CleanCQCpirDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    SCHEMA_LENGTH = len(Schemas.sample_schema)
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_pir_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=Schemas.sample_schema
        )
        self.test_cqc_pir_parquet_with_import_date = cleaning_utils.column_to_date(
            self.test_cqc_pir_parquet, Keys.import_date, CQCPIRClean.cqc_pir_import_date
        )
        self.test_add_care_home_column_df = self.spark.createDataFrame(
            Data.add_care_home_column_rows, Schemas.add_care_home_column_schema
        )
        self.test_expected_care_home_column_df = self.spark.createDataFrame(
            Data.expected_care_home_column_rows,
            Schemas.expected_care_home_column_schema,
        )

    @patch("jobs.clean_cqc_pir_data.filter_latest_submission_date")
    @patch("jobs.clean_cqc_pir_data.add_care_home_column")
    @patch("utils.cleaning_utils.column_to_date")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_patch,
        write_to_parquet_patch,
        column_to_date_patch,
        add_care_home_column,
        filter_latest_submission_date_patch,
    ):
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        read_from_parquet_patch.assert_called_once_with(self.TEST_SOURCE)
        self.assertTrue(column_to_date_patch.call_count, 2)
        add_care_home_column.assert_called_once_with(ANY)
        filter_latest_submission_date_patch.assert_called_once_with(ANY)

        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_remove_rows_without_people_directly_employed_removes_null_and_zero_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_rows_missing_people_directly_employed,
            Schemas.remove_rows_missing_people_directly_employed_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_rows_missing_people_directly_employed,
            Schemas.remove_rows_missing_people_directly_employed_schema,
        )
        returned_df = job.remove_rows_without_people_directly_employed(test_df)
        expected_df.show()
        returned_df.show()

        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_add_care_home_column_adds_a_column(self):
        returned_df = job.add_care_home_column(self.test_add_care_home_column_df)

        expected_df = self.test_expected_care_home_column_df

        self.assertCountEqual(expected_df.columns, returned_df.columns)

    def test_add_care_home_column_categorises_care_homes_correctly(self):
        returned_df = job.add_care_home_column(self.test_add_care_home_column_df)
        returned_data = returned_df.sort(CQCPIRClean.location_id).collect()
        expected_df = self.test_expected_care_home_column_df
        expected_data = expected_df.sort(CQCPIRClean.location_id).collect()

        self.assertCountEqual(expected_data[0], returned_data[0])
        self.assertCountEqual(expected_data[1], returned_data[1])
        self.assertCountEqual(expected_data[2], returned_data[2])
        self.assertCountEqual(expected_data[3], returned_data[3])

    def test_filter_latest_submission_date_returns_single_row_per_submission_date(self):
        test_df = self.spark.createDataFrame(
            CleanedData.subset_for_latest_submission_date_before_filter,
            CleanSchemas.clean_subset_for_grouping_by,
        )
        expected_df = self.spark.createDataFrame(
            CleanedData.subset_for_latest_submission_date_after_filter_deduplication,
            CleanSchemas.clean_subset_for_grouping_by,
        )

        test_df = job.filter_latest_submission_date(test_df)

        returned_data = test_df.sort(CQCPIRClean.cqc_pir_import_date).collect()
        expected_data = expected_df.sort(CQCPIRClean.cqc_pir_import_date).collect()

        self.assertCountEqual(expected_data[0], returned_data[0])
        self.assertCountEqual(expected_data[1], returned_data[1])
        self.assertCountEqual(
            expected_data[2],
            returned_data[2],
            "Row with Carehome indicator different has failed, and should not be removed",
        )
        self.assertCountEqual(expected_data[3], returned_data[3])

        self.assertTrue(test_df.count(), expected_df.count())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
