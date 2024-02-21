import unittest
import jobs.clean_cqc_pir_data as job

from unittest.mock import patch, ANY
from utils import utils, cleaning_utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPIRCleanedColumns as PIRClean,
)
from tests.test_file_schemas import CQCPIRSchema as Schemas
from tests.test_file_data import CQCpirData as Data


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
            self.test_cqc_pir_parquet, Keys.import_date, PIRClean.cqc_pir_import_date
        )
        self.test_add_care_home_column_df = self.spark.createDataFrame(
            Data.add_care_home_column_rows, Schemas.add_care_home_column_schema
        )
        self.test_expected_care_home_column_df = self.spark.createDataFrame(
            Data.expected_care_home_column_rows,
            Schemas.expected_care_home_column_schema,
        )

    @patch("jobs.clean_cqc_pir_data.add_care_home_column")
    @patch("utils.cleaning_utils.column_to_date")
    @patch("utils.utils.remove_already_cleaned_data")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_patch,
        write_to_parquet_patch,
        remove_already_cleaned_data_patch,
        column_to_date_patch,
        add_care_home_column,
    ):
        read_from_parquet_patch.return_value = self.test_cqc_pir_parquet
        remove_already_cleaned_data_patch.return_value = self.test_cqc_pir_parquet
        column_to_date_patch.return_value = self.test_cqc_pir_parquet_with_import_date

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        read_from_parquet_patch.assert_called_once_with(self.TEST_SOURCE)
        remove_already_cleaned_data_patch.assert_called_once_with(
            self.test_cqc_pir_parquet,
            self.TEST_DESTINATION,
        )
        column_to_date_patch.assert_called_once_with(
            self.test_cqc_pir_parquet,
            Keys.import_date,
            PIRClean.cqc_pir_import_date,
        )
        add_care_home_column.assert_called_once_with(
            self.test_cqc_pir_parquet_with_import_date
        )
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_add_care_home_column_adds_a_column(self):
        returned_df = job.add_care_home_column(self.test_add_care_home_column_df)

        expected_df = self.test_expected_care_home_column_df

        self.assertCountEqual(expected_df.columns, returned_df.columns)

    def test_add_care_home_column_categorises_care_homes_correctly(self):
        returned_df = job.add_care_home_column(self.test_add_care_home_column_df)
        returned_data = returned_df.sort(PIRClean.location_id).collect()
        expected_df = self.test_expected_care_home_column_df
        expected_data = expected_df.sort(PIRClean.location_id).collect()

        self.assertCountEqual(expected_data, returned_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
