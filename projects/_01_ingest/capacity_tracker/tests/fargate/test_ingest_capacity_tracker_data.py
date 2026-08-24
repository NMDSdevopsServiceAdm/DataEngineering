from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.capacity_tracker.fargate.ingest_capacity_tracker_data as job

PATCH_PATH = "projects._01_ingest.capacity_tracker.fargate.ingest_capacity_tracker_data"


class TestMain:
    @patch(f"{PATCH_PATH}.handle_job")
    @patch(f"{PATCH_PATH}.file_utils.get_s3_objects_list")
    def test_main_handles_single_csv_source(
        self, get_s3_objects_list_mock: Mock, handle_job_mock: Mock
    ):
        job.main("s3://bucket/some/path/file.csv", "s3://bucket/destination")

        get_s3_objects_list_mock.assert_not_called()
        handle_job_mock.assert_called_once_with(
            "s3://bucket/some/path/file.csv",
            "bucket",
            "some/path/file.csv",
            "s3://bucket/some/path",
        )

    @patch(f"{PATCH_PATH}.handle_job")
    @patch(f"{PATCH_PATH}.file_utils.get_s3_objects_list")
    def test_main_handles_a_directory_of_csvs(
        self, get_s3_objects_list_mock: Mock, handle_job_mock: Mock
    ):
        get_s3_objects_list_mock.return_value = [
            "some/path/file_one.csv",
            "some/path/file_two.csv",
        ]

        job.main("s3://bucket/some/path", "s3://bucket/destination")

        assert handle_job_mock.call_count == 2
        handle_job_mock.assert_any_call(
            "s3://bucket/some/path/file_one.csv",
            "bucket",
            "some/path/file_one.csv",
            "s3://bucket/some/path",
        )


class TestHandleJob:
    @patch(f"{PATCH_PATH}.ingest_dataset")
    @patch(f"{PATCH_PATH}.file_utils.identify_csv_delimiter")
    @patch(f"{PATCH_PATH}.file_utils.read_partial_csv_content")
    def test_handle_job_detects_delimiter_and_ingests(
        self,
        read_partial_csv_content_mock: Mock,
        identify_csv_delimiter_mock: Mock,
        ingest_dataset_mock: Mock,
    ):
        read_partial_csv_content_mock.return_value = "col1,col2\n1,2"
        identify_csv_delimiter_mock.return_value = ","

        job.handle_job("s3://bucket/file.csv", "bucket", "file.csv", "s3://bucket/dest")

        ingest_dataset_mock.assert_called_once_with(
            "s3://bucket/file.csv", "s3://bucket/dest", ","
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
    def test_replaces_spaces_and_removes_parentheses(self):
        test_lf = pl.LazyFrame(schema=["some col", "another(one)", "unchanged"])
        expected_lf = pl.LazyFrame(schema=["some_col", "anotherone", "unchanged"])

        returned_lf = job.sanitise_column_names(test_lf)

        assert (
            returned_lf.collect_schema().names() == expected_lf.collect_schema().names()
        )
