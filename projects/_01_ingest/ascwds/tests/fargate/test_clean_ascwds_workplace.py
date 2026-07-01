import unittest
from unittest.mock import ANY, Mock, call, patch

import pytest

import projects._01_ingest.ascwds.fargate.clean_ascwds_workplace as job

PATCH_PATH = "projects._01_ingest.ascwds.fargate.clean_ascwds_workplace"


class MainTests(unittest.TestCase):
    WORKPLACE_SOURCE = "some/source"
    CLEANED_WORKPLACE_DESTINATION = "some/destination"
    RECONCILIATION_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.wUtils.valid_workplace_filter")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        valid_filter_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.WORKPLACE_SOURCE,
            self.CLEANED_WORKPLACE_DESTINATION,
            self.RECONCILIATION_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            self.WORKPLACE_SOURCE, selected_columns=job.COLUMNS_TO_IMPORT
        )
        valid_filter_mock.assert_called_once()

        self.assertEqual(sink_to_parquet_mock.call_count, 2)

        sink_calls = [
            call(lazy_df=ANY, output_path=self.CLEANED_WORKPLACE_DESTINATION),
            call(lazy_df=ANY, output_path=self.RECONCILIATION_DESTINATION),
        ]
        sink_to_parquet_mock.assert_has_calls(sink_calls)
