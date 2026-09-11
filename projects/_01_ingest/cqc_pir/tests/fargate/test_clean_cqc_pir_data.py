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
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        add_care_home_column_mock: Mock,
        filter_latest_submission_date_mock: Mock,
        null_people_directly_employed_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        scan_parquet_mock.return_value = mock_lf
        column_to_date_mock.return_value = mock_lf
        add_care_home_column_mock.return_value = mock_lf
        filter_latest_submission_date_mock.return_value = mock_lf
        null_people_directly_employed_outliers_mock.return_value = mock_lf

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(
            self.TEST_SOURCE, schema=job.CQC_PIR_SCHEMA
        )
        assert column_to_date_mock.call_count == 2
        add_care_home_column_mock.assert_called_once()
        filter_latest_submission_date_mock.assert_called_once()
        null_people_directly_employed_outliers_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(ANY, self.TEST_DESTINATION)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cpUtils.null_people_directly_employed_outliers")
    @patch(f"{PATCH_PATH}.cpUtils.filter_latest_submission_date")
    @patch(f"{PATCH_PATH}.cpUtils.add_care_home_column")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_uses_submission_date_format_for_second_date_conversion(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        add_care_home_column_mock: Mock,
        filter_latest_submission_date_mock: Mock,
        null_people_directly_employed_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        scan_parquet_mock.return_value = mock_lf
        column_to_date_mock.return_value = mock_lf
        add_care_home_column_mock.return_value = mock_lf
        filter_latest_submission_date_mock.return_value = mock_lf
        null_people_directly_employed_outliers_mock.return_value = mock_lf

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        _, second_call_kwargs = column_to_date_mock.call_args_list[1]
        assert second_call_kwargs["format"] == job.PIR_SUBMISSION_DATE_FORMAT
