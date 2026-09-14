from unittest.mock import ANY, Mock, patch

import polars as pl

import projects._01_ingest.cqc_pir.fargate.clean_cqc_pir_data as job

PATCH_PATH = "projects._01_ingest.cqc_pir.fargate.clean_cqc_pir_data"


class TestMain:
    TEST_SOURCE = "some/source"
    TEST_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cpUtils.null_people_directly_employed_outliers")
    @patch(f"{PATCH_PATH}.cpUtils.filter_latest_submission_date")
    @patch(f"{PATCH_PATH}.cpUtils.add_care_home_column")
    @patch(f"{PATCH_PATH}.cUtils.cast_date_strings_to_dates")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        cast_date_strings_to_dates_mock: Mock,
        add_care_home_column_mock: Mock,
        filter_latest_submission_date_mock: Mock,
        null_people_directly_employed_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        scan_parquet_mock.return_value = mock_lf
        column_to_date_mock.return_value = mock_lf
        cast_date_strings_to_dates_mock.return_value = mock_lf
        add_care_home_column_mock.return_value = mock_lf
        filter_latest_submission_date_mock.return_value = mock_lf
        null_people_directly_employed_outliers_mock.return_value = mock_lf

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(
            self.TEST_SOURCE, schema=job.CQC_PIR_SCHEMA
        )
        column_to_date_mock.assert_called_once()
        cast_date_strings_to_dates_mock.assert_called_once()
        add_care_home_column_mock.assert_called_once()
        filter_latest_submission_date_mock.assert_called_once()
        null_people_directly_employed_outliers_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(ANY, self.TEST_DESTINATION)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cpUtils.null_people_directly_employed_outliers")
    @patch(f"{PATCH_PATH}.cpUtils.filter_latest_submission_date")
    @patch(f"{PATCH_PATH}.cpUtils.add_care_home_column")
    @patch(f"{PATCH_PATH}.cUtils.cast_date_strings_to_dates")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_parses_submission_date_with_its_own_format(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        cast_date_strings_to_dates_mock: Mock,
        add_care_home_column_mock: Mock,
        filter_latest_submission_date_mock: Mock,
        null_people_directly_employed_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        scan_parquet_mock.return_value = mock_lf
        column_to_date_mock.return_value = mock_lf
        cast_date_strings_to_dates_mock.return_value = mock_lf
        add_care_home_column_mock.return_value = mock_lf
        filter_latest_submission_date_mock.return_value = mock_lf
        null_people_directly_employed_outliers_mock.return_value = mock_lf

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        _, kwargs = cast_date_strings_to_dates_mock.call_args
        assert (
            kwargs["date_column_identifier"] == job.PIRClean.pir_submission_date_as_date
        )
        assert kwargs["raw_date_format"] == job.PIR_SUBMISSION_DATE_FORMAT
