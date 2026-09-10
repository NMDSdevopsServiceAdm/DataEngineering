from datetime import date
from unittest.mock import Mock, patch

import polars as pl
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate._02_clean"

TEST_SOURCE = "some/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        date_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {IndCQC.care_home_status_count: [1, 2]}
        )
        date_mock.today.return_value = date(2026, 9, 1)
        date_mock.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        reduced_data_filter_expr_mock.return_value = pl.lit(True)

        job.main(TEST_SOURCE, TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)

        reduced_data_filter_expr_mock.assert_called_once_with(
            cutoff_date=date(2020, 4, 1),
        )

        sink_call_kwargs = sink_to_parquet_mock.call_args.kwargs
        assert sink_call_kwargs["output_path"] == TEST_DESTINATION
        expected_lf = pl.LazyFrame(
            {
                IndCQC.care_home_status_count: [1, 2],
                Pub.consistent_service: [True, False],
            }
        )
        assert_frame_equal(sink_call_kwargs["lazy_df"], expected_lf)
