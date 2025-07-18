import unittest

from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.jobs.validate_providers_api_raw_delta_data as job

from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidateProvidersAPIRawData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidateProvidersAPIRawData as Schemas,
)

from utils import utils


class ValidateProvidersAPIRawDatasetDeltaTests(unittest.TestCase):
    TEST_RAW_CQC_PROVIDER_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_raw_cqc_provider_df = self.spark.createDataFrame(
            Data.raw_cqc_providers_rows,
            Schemas.raw_cqc_providers_schema,
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateProvidersAPIRawDatasetDeltaTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_raw_cqc_provider_df

        job.main(
            self.TEST_RAW_CQC_PROVIDER_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
