import unittest
from unittest.mock import ANY, Mock, patch

import projects._01_ingest.cqc_api.fargate.cqc_providers_4_full_clean as job

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.cqc_providers_4_full_clean"


class CqcProvidersFullCleanTests(unittest.TestCase):
    TEST_FLATTENED_SOURCE = "some/cqc/source"
    TEST_CLEAN_DESTINATION = "some/reg/destination"
    TEST_DEREG_DESTINATION = "some/reg/destination"
    TEST_MANUAL_POSTCODE_CORRETIONS_SOURCE = "some/ons/source"

    mock_cqc_providers_data = Mock(name="cqc_providers_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_cqc_providers_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.TEST_FLATTENED_SOURCE, self.TEST_CLEAN_DESTINATION)

        scan_parquet_mock.assert_called_once_with(
            self.TEST_FLATTENED_SOURCE,
            schema=job.POLARS_PROVIDER_SCHEMA,
            selected_columns=job.cqc_provider_cols_to_import,
        )
        column_to_date_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY, self.TEST_CLEAN_DESTINATION, append=False
        )
