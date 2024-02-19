import unittest
import jobs.clean_cqc_pir_data as job

from unittest.mock import patch, ANY
from utils import utils, cleaning_utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcLPIRCleanedColumns as PIRClean,
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
    ):
        read_from_parquet_patch.return_value = self.test_cqc_pir_parquet
        remove_already_cleaned_data_patch.return_value = self.test_cqc_pir_parquet

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
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    @patch("utils.cleaning_utils.column_to_date")
    @patch("utils.utils.remove_already_cleaned_data")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_correct_number_of_columns_written(
        self,
        read_from_parquet_patch,
        write_to_parquet_patch,
        remove_already_cleaned_data_patch,
        column_to_date_patch,
    ):
        read_from_parquet_patch.return_value = self.test_cqc_pir_parquet
        remove_already_cleaned_data_patch.return_value = self.test_cqc_pir_parquet
        column_to_date_patch.return_value = self.test_cqc_pir_parquet_with_import_date

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        write_to_parquet_patch.assert_called_once_with(
            self.test_cqc_pir_parquet_with_import_date,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )
        self.assertEqual(
            self.SCHEMA_LENGTH + 1,
            len(self.test_cqc_pir_parquet_with_import_date.columns),
        )
        self.assertTrue(
            self.test_cqc_pir_parquet_with_import_date.columns.index(
                PIRClean.cqc_pir_import_date
            )
            == self.SCHEMA_LENGTH  # The last index of the df
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
