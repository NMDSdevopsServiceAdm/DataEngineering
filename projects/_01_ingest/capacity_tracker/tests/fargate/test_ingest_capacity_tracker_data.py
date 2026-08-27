from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.capacity_tracker.fargate.ingest_capacity_tracker_data as job
from projects._01_ingest.capacity_tracker.unittest_data.capacity_tracker_test_file_data import (
    SANITISE_COLUMN_NAMES_DOES_NOT_SNAKE_CASE_EXPECTED_SCHEMA,
    SANITISE_COLUMN_NAMES_DOES_NOT_SNAKE_CASE_INPUT_SCHEMA,
    SANITISE_COLUMN_NAMES_EXPECTED_SCHEMA,
    SANITISE_COLUMN_NAMES_INPUT_SCHEMA,
)

PATCH_PATH = "projects._01_ingest.capacity_tracker.fargate.ingest_capacity_tracker_data"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_main_sinks_to_a_path_derived_from_source_key_and_destination_prefix(
        self, scan_csv_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_csv_mock.return_value = pl.LazyFrame({"col (1)": [1], "col 2": [2]})

        job.main("s3://bucket/some/path/file.csv", "s3://bucket/destination")

        scan_csv_mock.assert_called_once_with(
            "s3://bucket/some/path/file.csv", separator=",", infer_schema=False
        )
        sink_to_parquet_mock.assert_called_once()
        sunk_lf = sink_to_parquet_mock.call_args[0][0]
        pl_testing.assert_frame_equal(
            sunk_lf, pl.LazyFrame({"col_1": [1], "col_2": [2]})
        )
        assert sink_to_parquet_mock.call_args[0][1] == "s3://bucket/some/path/"


class TestSanitiseColumnNames:
    def test_replaces_spaces_removes_parentheses_and_lowercases(self):
        test_lf = pl.LazyFrame(schema=SANITISE_COLUMN_NAMES_INPUT_SCHEMA)
        expected_lf = pl.LazyFrame(schema=SANITISE_COLUMN_NAMES_EXPECTED_SCHEMA)

        returned_lf = job.sanitise_column_names(test_lf)

        assert (
            returned_lf.collect_schema().names() == expected_lf.collect_schema().names()
        )

    def test_does_not_insert_underscores_at_word_boundaries(self):
        # capacity_tracker_columns names multi-word columns as one smooshed lowercase
        # word (e.g. "cqccareworkersemployed"), so sanitising must only lowercase and
        # strip invalid characters, not snake_case the PascalCase header.
        test_lf = pl.LazyFrame(
            schema=SANITISE_COLUMN_NAMES_DOES_NOT_SNAKE_CASE_INPUT_SCHEMA
        )
        expected_lf = pl.LazyFrame(
            schema=SANITISE_COLUMN_NAMES_DOES_NOT_SNAKE_CASE_EXPECTED_SCHEMA
        )

        returned_lf = job.sanitise_column_names(test_lf)

        assert (
            returned_lf.collect_schema().names() == expected_lf.collect_schema().names()
        )
