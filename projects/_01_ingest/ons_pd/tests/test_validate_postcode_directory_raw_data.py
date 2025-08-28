import unittest

from unittest.mock import Mock, patch

import projects._01_ingest.ons_pd.jobs.validate_postcode_directory_raw_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidatePostcodeDirectoryRawData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidatePostcodeDirectoryRawData as Schemas,
)
from utils import utils

PATCH_PATH = "projects._01_ingest.ons_pd.jobs.validate_postcode_directory_raw_data"


class ValidatePostcodeDirectoryRawDatasetTests(unittest.TestCase):
    TEST_RAW_POSTCODE_DIRECTORY_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_raw_postcode_directory_df = self.spark.createDataFrame(
            Data.raw_postcode_directory_rows,
            Schemas.raw_postcode_directory_schema,
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidatePostcodeDirectoryRawDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_raw_postcode_directory_df

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_RAW_POSTCODE_DIRECTORY_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 1)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
