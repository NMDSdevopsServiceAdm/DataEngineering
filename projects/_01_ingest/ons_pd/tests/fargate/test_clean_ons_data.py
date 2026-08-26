from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.ons_pd.fargate.clean_ons_data as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    CleanOnsDataTest,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)

PATCH_PATH = "projects._01_ingest.ons_pd.fargate.clean_ons_data"


class TestMain:
    TEST_SOURCE = "some/source"
    TEST_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        apply_categorical_labels_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        scan_parquet_mock.return_value = mock_lf
        column_to_date_mock.return_value = mock_lf
        apply_categorical_labels_mock.return_value = mock_lf

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(
            self.TEST_SOURCE, schema=job.ONS_SCHEMA
        )
        column_to_date_mock.assert_called_once()
        apply_categorical_labels_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY, output_path=self.TEST_DESTINATION
        )


class TestPrepareContemporaryOnsData:
    def test_returns_all_rows_renamed_without_2021_rui(self):
        input_lf = pl.LazyFrame(CleanOnsDataTest.prepare_ons_data_rows)

        returned_lf = job.prepare_contemporary_ons_data(input_lf)

        expected_lf = pl.LazyFrame(
            CleanOnsDataTest.expected_prepare_contemporary_ons_data
        )
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, check_row_order=False
        )


class TestPrepareCurrentOnsData:
    def test_returns_only_max_date_rows_renamed_with_2021_rui(self):
        input_lf = pl.LazyFrame(CleanOnsDataTest.prepare_ons_data_rows)

        returned_lf = job.prepare_current_ons_data(input_lf)

        expected_lf = pl.LazyFrame(CleanOnsDataTest.expected_prepare_current_ons_data)
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, check_row_order=False
        )

    def test_every_row_shares_the_max_import_date(self):
        input_lf = pl.LazyFrame(CleanOnsDataTest.prepare_ons_data_rows)

        returned_df = job.prepare_current_ons_data(input_lf).collect()

        assert returned_df[ONSClean.current_ons_import_date].n_unique() == 1


class TestBuildLabelsLf:
    def test_flattens_column_to_code_to_label_dict_into_rows(self):
        returned_lf = job.build_labels_lf(
            CleanOnsDataTest.labels_dict_for_build_labels_lf
        )

        expected_lf = pl.LazyFrame(CleanOnsDataTest.expected_build_labels_lf)
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, check_row_order=False
        )
