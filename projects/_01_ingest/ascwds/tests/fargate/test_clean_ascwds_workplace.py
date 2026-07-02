import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import pytest

import projects
import projects._01_ingest.ascwds.fargate.clean_ascwds_workplace as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.clean_ascwds_workplace"


class MainTests(unittest.TestCase):
    WORKPLACE_SOURCE = "some/source"
    DATA_LABELS_SOURCE = "some/labels/source"
    CLEANED_WORKPLACE_DESTINATION = "some/destination"
    WORKPLACE_FOR_RECONCILIATION_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        scan_csv_mock: Mock,
        apply_categorical_labels_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.WORKPLACE_SOURCE,
            self.DATA_LABELS_SOURCE,
            self.CLEANED_WORKPLACE_DESTINATION,
            self.WORKPLACE_FOR_RECONCILIATION_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            self.WORKPLACE_SOURCE,
            selected_columns=job.ascwds_workplace_columns_to_import,
        )
        scan_csv_mock.assert_called_once_with(
            self.DATA_LABELS_SOURCE, schema=job.data_labels_schema
        )
        apply_categorical_labels_mock.assert_called_once()

        assert sink_to_parquet_mock.call_count == 2

        sink_calls = [
            call(lazy_df=ANY, output_path=self.CLEANED_WORKPLACE_DESTINATION),
            call(
                lazy_df=ANY, output_path=self.WORKPLACE_FOR_RECONCILIATION_DESTINATION
            ),
        ]
        sink_to_parquet_mock.assert_has_calls(sink_calls)
