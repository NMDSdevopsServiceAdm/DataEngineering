import unittest
from unittest.mock import ANY, Mock, call, patch

import pytest

import projects._01_ingest.ascwds.fargate.clean_ascwds_workplace as job

PATCH_PATH = "projects._01_ingest.ascwds.fargate.clean_ascwds_workplace"


class MainTests(unittest.TestCase):
    WORKPLACE_SOURCE = "some/source"
    CLEANED_WORKPLACE_DESTINATION = "some/destination"
    WORKPLACE_FOR_RECONCILIATION_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.WORKPLACE_SOURCE,
            self.CLEANED_WORKPLACE_DESTINATION,
            self.WORKPLACE_FOR_RECONCILIATION_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            self.WORKPLACE_SOURCE, selected_columns=job.columns_to_import
        )

        assert sink_to_parquet_mock.call_count == 2

        sink_calls = [
            call(lazy_df=ANY, output_path=self.CLEANED_WORKPLACE_DESTINATION),
            call(
                lazy_df=ANY, output_path=self.WORKPLACE_FOR_RECONCILIATION_DESTINATION
            ),
        ]
        sink_to_parquet_mock.assert_has_calls(sink_calls)
