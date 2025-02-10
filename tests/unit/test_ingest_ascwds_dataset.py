import unittest
from unittest.mock import call, patch, Mock

import jobs.ingest_ascwds_dataset as job
from tests.test_file_data import IngestASCWDSData as Data
from tests.test_file_schemas import IngestASCWDSData as Schemas
from utils import utils


class IngestASCWDSDatasetTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.single_csv_file_source = "s3://bucket/source.csv"
        self.bucket = "bucket"
        self.csv_file_name = "source.csv"
        self.destination_path = "s3://bucket/destination/"


class IngestSingleFileTest(IngestASCWDSDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.construct_destination_path")
    @patch("jobs.ingest_ascwds_dataset.handle_job")
    def test_ingest_single_file(
        self, handle_job_mock: Mock, construct_destination_path_mock: Mock
    ):
        expected_new_destination = "s3://bucket/destination/source.csv"
        construct_destination_path_mock.return_value = expected_new_destination

        job.ingest_single_file(
            self.single_csv_file_source,
            self.bucket,
            self.csv_file_name,
            self.destination_path,
        )

        construct_destination_path_mock.assert_called_once_with(
            self.destination_path, self.csv_file_name
        )
        handle_job_mock.assert_called_once_with(
            self.single_csv_file_source,
            self.bucket,
            self.csv_file_name,
            expected_new_destination,
        )


class IngestMultipleFilesTests(IngestASCWDSDatasetTests):
    @patch("utils.utils.construct_s3_uri")
    @patch("utils.utils.construct_destination_path")
    @patch("utils.utils.get_s3_objects_list")
    @patch("jobs.ingest_ascwds_dataset.handle_job")
    def test_ingest_multiple_files(
        self,
        handle_job_mock: Mock,
        get_s3_objects_list_mock: Mock,
        construct_destination_path_mock: Mock,
        construct_s3_uri_mock: Mock,
    ):
        prefix = "prefix/"
        file1 = "file1.csv"
        file2 = "file2.csv"
        objects_list = [file1, file2]

        get_s3_objects_list_mock.return_value = objects_list
        construct_s3_uri_mock.side_effect = (
            lambda bucket, prefix: f"s3://{bucket}/{prefix}"
        )
        construct_destination_path_mock.side_effect = (
            lambda destination, prefix: f"{destination}{prefix}"
        )

        job.ingest_multiple_files(self.bucket, prefix, self.destination_path)

        get_s3_objects_list_mock.assert_called_once_with(self.bucket, prefix)
        expected_calls_s3_uri = [
            call(self.bucket, file1),
            call(self.bucket, file2),
        ]
        construct_s3_uri_mock.assert_has_calls(expected_calls_s3_uri)
        expected_calls_destination_path = [
            call(self.destination_path, file1),
            call(self.destination_path, file2),
        ]
        construct_destination_path_mock.assert_has_calls(
            expected_calls_destination_path
        )
        expected_calls_handle_job = [
            call(
                "s3://bucket/file1.csv",
                self.bucket,
                file1,
                "s3://bucket/destination/file1.csv",
            ),
            call(
                "s3://bucket/file2.csv",
                self.bucket,
                file2,
                "s3://bucket/destination/file2.csv",
            ),
        ]
        handle_job_mock.assert_has_calls(expected_calls_handle_job)


class TestHandleJob(IngestASCWDSDatasetTests):
    @patch("utils.utils.read_partial_csv_content")
    @patch("utils.utils.identify_csv_delimiter")
    @patch("utils.utils.read_csv")
    @patch("utils.utils.write_to_parquet")
    @patch("jobs.ingest_ascwds_dataset.raise_error_if_mainjrid_includes_unknown_values")
    def test_handle_job(
        self,
        raise_error_mock: Mock,
        write_to_parquet_mock: Mock,
        read_csv_mock: Mock,
        identify_csv_delimiter_mock: Mock,
        read_partial_csv_content_mock: Mock,
    ):
        file_sample = "sample_data"
        delimiter = ","
        df = "dataframe"

        read_partial_csv_content_mock.return_value = file_sample
        identify_csv_delimiter_mock.return_value = delimiter
        read_csv_mock.return_value = df
        raise_error_mock.return_value = df

        job.handle_job(
            self.single_csv_file_source,
            self.bucket,
            self.csv_file_name,
            self.destination_path,
        )

        read_partial_csv_content_mock.assert_called_once_with(
            self.bucket, self.csv_file_name
        )
        identify_csv_delimiter_mock.assert_called_once_with(file_sample)
        read_csv_mock.assert_called_once_with(self.single_csv_file_source, delimiter)
        raise_error_mock.assert_called_once_with(df)
        write_to_parquet_mock.assert_called_once_with(df, self.destination_path)


class RaiseErrorIfMainjridIncludesUnknownValuesTests(IngestASCWDSDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.ascwds_without_mainjrid_df = self.spark.createDataFrame(
            Data.raise_mainjrid_error_col_not_present_rows,
            Schemas.raise_mainjrid_error_when_mainjrid_not_in_df_schema,
        )
        self.ascwds_with_known_mainjrid_df = self.spark.createDataFrame(
            Data.raise_mainjrid_error_with_known_value_rows,
            Schemas.raise_mainjrid_error_when_mainjrid_in_df_schema,
        )

    def test_error_not_raised_if_mainjrid_column_not_present(
        self,
    ):
        try:
            job.raise_error_if_mainjrid_includes_unknown_values(
                self.ascwds_without_mainjrid_df
            )
        except ValueError:
            self.fail(
                "raise_error_if_mainjrid_includes_unknown_values() raised ValueError unexpectedly"
            )

    def test_returns_original_df_if_mainjrid_column_not_present(
        self,
    ):
        returned_df = job.raise_error_if_mainjrid_includes_unknown_values(
            self.ascwds_without_mainjrid_df
        )
        self.assertEqual(
            returned_df.collect(), self.ascwds_without_mainjrid_df.collect()
        )

    def test_error_not_raised_if_mainjrid_present_and_all_values_known(
        self,
    ):
        try:
            job.raise_error_if_mainjrid_includes_unknown_values(
                self.ascwds_with_known_mainjrid_df
            )
        except ValueError:
            self.fail(
                "raise_error_if_mainjrid_includes_unknown_values() raised ValueError unexpectedly"
            )

    def test_returns_original_df_if_mainjrid_present_and_all_values_known(
        self,
    ):
        returned_df = job.raise_error_if_mainjrid_includes_unknown_values(
            self.ascwds_with_known_mainjrid_df
        )
        self.assertEqual(
            returned_df.collect(), self.ascwds_with_known_mainjrid_df.collect()
        )

    def test_raises_error_if_mainjrid_includes_unknown_values(self):
        test_df = self.spark.createDataFrame(
            Data.raise_mainjrid_error_with_unknown_value_rows,
            Schemas.raise_mainjrid_error_when_mainjrid_in_df_schema,
        )

        with self.assertRaises(ValueError) as context:
            job.raise_error_if_mainjrid_includes_unknown_values(test_df)

        self.assertIn(
            "Error: this file contains 1 unknown mainjrid record(s)",
            str(context.exception),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
