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
    RECONCILIATION_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.wUtils.create_purged_lfs_for_reconciliation_and_data")
    @patch(f"{PATCH_PATH}.cUtils.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.wUtils.remove_rows_with_duplicate_location_ids")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.cUtils.cast_date_strings_to_dates")
    @patch(f"{PATCH_PATH}.wUtils.valid_workplace_filter")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        valid_filter_mock: Mock,
        cast_date_strings_to_dates_mock: Mock,
        column_to_date_mock: Mock,
        remove_rows_with_duplicate_location_ids_mock: Mock,
        scan_csv_mock: Mock,
        apply_categorical_labels_mock: Mock,
        create_purged_lfs_for_reconciliation_and_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        create_purged_lfs_for_reconciliation_and_data_mock.return_value = (
            Mock(),
            Mock(),
        )
        job.main(
            self.WORKPLACE_SOURCE,
            self.DATA_LABELS_SOURCE,
            self.CLEANED_WORKPLACE_DESTINATION,
            self.RECONCILIATION_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            self.WORKPLACE_SOURCE, selected_columns=job.COLUMNS_TO_IMPORT
        )
        valid_filter_mock.assert_called_once()

        cast_date_strings_to_dates_mock.assert_called_once()
        column_to_date_mock.assert_called_once()
        remove_rows_with_duplicate_location_ids_mock.assert_called_once()

        create_purged_lfs_for_reconciliation_and_data_mock.assert_called_once()

        create_purged_lfs_for_reconciliation_and_data_mock.assert_called_once()

        scan_csv_mock.assert_called_once_with(
            self.DATA_LABELS_SOURCE, schema=job.data_labels_schema
        )
        apply_categorical_labels_mock.assert_called_once()

        assert sink_to_parquet_mock.call_count == 2

        sink_calls = [
            call(lazy_df=ANY, output_path=self.CLEANED_WORKPLACE_DESTINATION),
            call(lazy_df=ANY, output_path=self.RECONCILIATION_DESTINATION),
        ]
        sink_to_parquet_mock.assert_has_calls(sink_calls)


class SLVColumnsTests(unittest.TestCase):
    def setUp(self) -> None:
        slv_cols_selector = job.slv_columns

        test_lf = pl.LazyFrame(
            {
                AWPClean.job_role_01_agency: "1",
                AWPClean.job_role_01_employees: "1",
                AWPClean.job_role_01_flag: "1",
                "any_other_col": "1",
            }
        )
        selected_lf = test_lf.select(slv_cols_selector)
        self.selected_cols = selected_lf.collect_schema().names()

    def test_selected_cols_contains_strings_starting_with_jr(self):
        for i in self.selected_cols:
            assert i.startswith("jr")

    def test_list_contains_strings_with_expected_endings(self):
        string_endings_1 = [i[-4:] for i in self.selected_cols if len(i) == 8]
        string_endings_2 = [i[-3:] for i in self.selected_cols if len(i) == 7]
        string_endings_3 = set(string_endings_1 + string_endings_2)
        expected_endings = {"agcy", "emp"}
        self.assertEqual(string_endings_3, expected_endings)

    def test_list_does_not_contain_strings_with_flag(self):
        for i in self.selected_cols:
            assert "flag" not in i

    def test_list_is_expected_length(self):
        self.assertEqual(len(self.selected_cols), 2)
