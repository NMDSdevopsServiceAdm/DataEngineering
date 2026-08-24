from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.capacity_tracker.fargate.ingest_capacity_tracker_data as job

PATCH_PATH = "projects._01_ingest.capacity_tracker.fargate.ingest_capacity_tracker_data"

SOURCE_KEY = (
    "domain=capacity_tracker/dataset=capacity_tracker_care_home/"
    "year=2026/month=08/day=01/import_date=20260801/file.csv"
)


class TestMain:
    @patch(f"{PATCH_PATH}.ingest_dataset")
    @patch(f"{PATCH_PATH}.file_utils.identify_csv_delimiter")
    @patch(f"{PATCH_PATH}.file_utils.read_partial_csv_content")
    def test_main_detects_delimiter_and_ingests_with_partition_path_preserved(
        self,
        read_partial_csv_content_mock: Mock,
        identify_csv_delimiter_mock: Mock,
        ingest_dataset_mock: Mock,
    ):
        read_partial_csv_content_mock.return_value = "col1,col2\n1,2"
        identify_csv_delimiter_mock.return_value = ","
        source = f"s3://bucket/{SOURCE_KEY}"

        job.main(
            source,
            "s3://bucket/domain=capacity_tracker/dataset=capacity_tracker_care_home_polars",
        )

        read_partial_csv_content_mock.assert_called_once_with("bucket", SOURCE_KEY)
        ingest_dataset_mock.assert_called_once_with(
            source,
            "s3://bucket/domain=capacity_tracker/dataset=capacity_tracker_care_home_polars/"
            "year=2026/month=08/day=01/import_date=20260801/",
            ",",
        )


class TestPartitionPathFromKey:
    def test_returns_path_after_dataset_segment_with_trailing_slash(self):
        assert (
            job.partition_path_from_key(SOURCE_KEY)
            == "year=2026/month=08/day=01/import_date=20260801/"
        )


class TestIngestDataset:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_ingest_dataset_sinks_sanitised_columns_with_trailing_slash(
        self, scan_csv_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_csv_mock.return_value = pl.LazyFrame({"col (1)": [1], "col 2": [2]})

        job.ingest_dataset("s3://bucket/file.csv", "s3://bucket/dest", ",")

        scan_csv_mock.assert_called_once_with(
            "s3://bucket/file.csv", separator=",", infer_schema=False
        )
        sunk_lf = sink_to_parquet_mock.call_args[0][0]
        pl_testing.assert_frame_equal(
            sunk_lf, pl.LazyFrame({"col_1": [1], "col_2": [2]})
        )
        assert sink_to_parquet_mock.call_args[0][1] == "s3://bucket/dest/"


class TestSanitiseColumnNames:
    def test_replaces_spaces_removes_parentheses_and_lowercases(self):
        test_lf = pl.LazyFrame(
            schema=["Some Col", "Another(One)", "unchanged", "CqcId"]
        )
        expected_lf = pl.LazyFrame(
            schema=["some_col", "anotherone", "unchanged", "cqcid"]
        )

        returned_lf = job.sanitise_column_names(test_lf)

        assert (
            returned_lf.collect_schema().names() == expected_lf.collect_schema().names()
        )
