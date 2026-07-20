import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.ascwds.fargate.clean_ascwds_workplace as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.clean_ascwds_workplace"


class MainTests(unittest.TestCase):
    WORKPLACE_SOURCE = "some/source"
    DATA_LABELS_SOURCE = "some/labels/source"
    CLEANED_WORKPLACE_DESTINATION = "some/destination"
    SFC_INTERNAL_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.wUtils.remove_rows_with_duplicate_location_ids")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.discover_combined_schema")
    @patch(f"{PATCH_PATH}.wUtils.create_purge_date_columns")
    @patch(f"{PATCH_PATH}.cUtils.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.cUtils.cast_date_strings_to_dates")
    @patch(f"{PATCH_PATH}.wUtils.valid_workplace_filter")
    @patch(f"{PATCH_PATH}.wUtils.apply_data_corrections")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        apply_data_corrections_mock: Mock,
        valid_filter_mock: Mock,
        cast_date_strings_to_dates_mock: Mock,
        column_to_date_mock: Mock,
        scan_csv_mock: Mock,
        apply_categorical_labels_mock: Mock,
        create_purge_date_columns_mock: Mock,
        discover_combined_schema_mock: Mock,
        sink_to_parquet_mock: Mock,
        remove_rows_with_duplicate_location_ids_mock: Mock,
    ):
        discover_combined_schema_mock.return_value = {
            "jr09emp": pl.String,
            AWPClean.establishment_id: pl.String,
        }

        job.main(
            self.WORKPLACE_SOURCE,
            self.DATA_LABELS_SOURCE,
            self.CLEANED_WORKPLACE_DESTINATION,
            self.SFC_INTERNAL_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 2
        assert scan_parquet_mock.call_args_list[0] == call(
            self.WORKPLACE_SOURCE, schema=job.WORKPLACE_SCHEMA
        )
        assert scan_parquet_mock.call_args_list[1] == call(
            self.WORKPLACE_SOURCE,
            schema=pl.Schema(
                {"jr09emp": pl.String, AWPClean.establishment_id: pl.String}
            ),
        )

        apply_data_corrections_mock.assert_called_once()
        valid_filter_mock.assert_called_once()
        cast_date_strings_to_dates_mock.assert_called_once()
        column_to_date_mock.assert_called_once()
        scan_csv_mock.assert_called_once_with(
            self.DATA_LABELS_SOURCE, schema=job.data_labels_schema
        )
        apply_categorical_labels_mock.assert_called_once()
        create_purge_date_columns_mock.assert_called_once()
        remove_rows_with_duplicate_location_ids_mock.assert_called_once()
        discover_combined_schema_mock.assert_called_once_with(self.WORKPLACE_SOURCE)

        assert sink_to_parquet_mock.call_count == 2
        sink_to_parquet_mock.assert_has_calls(
            [
                call(ANY, output_path=self.SFC_INTERNAL_DESTINATION),
                call(ANY, output_path=self.CLEANED_WORKPLACE_DESTINATION),
            ]
        )
