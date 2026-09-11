from datetime import date
from unittest.mock import Mock, call, patch

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
    @patch(f"{PATCH_PATH}.clean_utils.has_continuous_data_since_date")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        date_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        has_continuous_data_since_date_mock: Mock,
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
        merged_lf.filter.assert_called_once_with(
            reduced_data_filter_expr_mock.return_value
        )

        coalesce_expr = merged_lf.filter.return_value.with_columns.call_args_list[
            0
        ].args[0]
        assert set(coalesce_expr.meta.root_names()) == {
            IndCQC.ct_care_home_total_employed_imputed,
            IndCQC.ct_non_res_care_workers_employed_imputed,
        }
        assert coalesce_expr.meta.output_name() == Pub.ct_total_employed_imputed

        has_continuous_data_since_date_mock.assert_has_calls(
            [
                call(
                    Pub.ct_total_employed_imputed,
                    date(2021, 7, 1),
                    Pub.ct_has_data_long_term,
                ),
                call(
                    Pub.ct_total_employed_imputed,
                    date(2025, 4, 1),
                    Pub.ct_has_data_medium_term,
                ),
                call(
                    Pub.ct_total_employed_imputed,
                    date(2026, 4, 1),
                    Pub.ct_has_data_short_term,
                ),
            ]
        )
        coalesced_lf = merged_lf.filter.return_value.with_columns.return_value
        coalesced_lf.with_columns.assert_called_once_with(
            has_continuous_data_since_date_mock.return_value,
            has_continuous_data_since_date_mock.return_value,
            has_continuous_data_since_date_mock.return_value,
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=coalesced_lf.with_columns.return_value,
            output_path=TEST_DESTINATION,
        )
