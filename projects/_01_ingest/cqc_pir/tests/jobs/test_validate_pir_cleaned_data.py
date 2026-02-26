import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_pir.jobs.validate_pir_cleaned_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidatePIRCleanedData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidatePIRCleanedData as Schemas,
)
from tests.base_test import SparkBaseTest

PATCH_PATH: str = "projects._01_ingest.cqc_pir.jobs.validate_pir_cleaned_data"


class ValidatePIRCleanedDatasetTests(SparkBaseTest):
    TEST_CQC_PIR_CLEANED_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.test_cleaned_cqc_pir_df = self.spark.createDataFrame(
            Data.cleaned_cqc_pir_rows, Schemas.cleaned_cqc_pir_schema
        )


class MainTests(ValidatePIRCleanedDatasetTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cleaned_cqc_pir_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_CQC_PIR_CLEANED_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 1)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
