import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_pir.jobs.validate_pir_raw_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidatePIRRawData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidatePIRRawData as Schemas,
)
from tests.base_test import SparkBaseTest

PATCH_PATH: str = "projects._01_ingest.cqc_pir.jobs.validate_pir_raw_data"


class ValidatePIRRawDatasetTests(SparkBaseTest):
    TEST_CQC_PIR_RAW_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.test_raw_cqc_pir_df = self.spark.createDataFrame(
            Data.raw_cqc_pir_rows, Schemas.raw_cqc_pir_schema
        )


class MainTests(ValidatePIRRawDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_raw_cqc_pir_df

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_CQC_PIR_RAW_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 1)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
