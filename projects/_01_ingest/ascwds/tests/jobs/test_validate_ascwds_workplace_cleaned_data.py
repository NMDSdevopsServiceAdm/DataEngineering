import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.ascwds.jobs.validate_ascwds_workplace_cleaned_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidateASCWDSWorkplaceCleanedData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidateASCWDSWorkplaceCleanedData as Schemas,
)
from tests.base_test import SparkBaseTest


class ValidateASCWDSWorkplaceCleanedDatasetTests(SparkBaseTest):
    TEST_ASCWDS_WORKPLACE_CLEANED_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.test_cleaned_ascwds_workplace_df = self.spark.createDataFrame(
            Data.cleaned_ascwds_workplace_rows, Schemas.cleaned_ascwds_workplace_schema
        )


class MainTests(ValidateASCWDSWorkplaceCleanedDatasetTests):
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cleaned_ascwds_workplace_df,
        ]

        job.main(
            self.TEST_ASCWDS_WORKPLACE_CLEANED_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
