import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class MainTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    PREPARED_JOB_ROLE_COUNTS_SOURCE = "some/other/source"
    ESTIMATES_DESTINATION = "some/destination"

    mock_estimate_data = Mock(name="estimate_data")
    mock_prepared_job_role_counts_data = Mock(name="prepared_job_role_counts_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(
        f"{PATCH_PATH}.JRUtils.get_estimated_managers_diff_from_cqc_registered_managers"
    )
    @patch(f"{PATCH_PATH}.JRUtils.cap_registered_managers_to_1")
    @patch(f"{PATCH_PATH}.coalesce_ratios_with_source_label")
    @patch(f"{PATCH_PATH}.JRUtils.rolling_sum_of_job_role_counts")
    @patch(f"{PATCH_PATH}.JRUtils.impute_full_time_series")
    @patch(f"{PATCH_PATH}.JRUtils.percentage_share")
    @patch(f"{PATCH_PATH}.JRUtils.nullify_job_role_count_when_source_not_ascwds")
    @patch(f"{PATCH_PATH}.JRUtils.join_worker_to_estimates_dataframe")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        return_value=[mock_estimate_data, mock_prepared_job_role_counts_data],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_worker_to_estimates_dataframe_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        percentage_share_mock: Mock,
        impute_full_time_series_mock: Mock,
        rolling_sum_mock: Mock,
        coalesce_ratios_with_source_label_mock: Mock,
        cap_registered_managers_to_1_mock: Mock,
        get_estimated_managers_diff_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.ESTIMATE_SOURCE,
            self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
            self.ESTIMATES_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    source=self.ESTIMATE_SOURCE,
                    selected_columns=job.estimates_columns_to_import,
                ),
                call(
                    source=self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
                    selected_columns=job.ascwds_columns_to_import,
                ),
            ]
        )

        join_worker_to_estimates_dataframe_mock.assert_called_once()
        nullify_job_role_count_when_source_not_ascwds_mock.assert_called_once()

        calls = [
            call(IndCQC.ascwds_job_role_counts),
            call(IndCQC.ascwds_job_role_rolling_sum),
        ]
        percentage_share_mock.assert_has_calls(calls, any_order=True)
        pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
        # Assert that we're getting the percentage_share over the required groups.
        percentage_share_mock.return_value.over.assert_has_calls(
            [call(pct_share_groups)] * 2,
            any_order=True,
        )

        impute_full_time_series_mock.assert_called_once_with(
            IndCQC.ascwds_job_role_ratios
        )
        rolling_sum_mock.assert_called_once_with(period="6mo")
        coalesce_ratios_with_source_label_mock.assert_called_once()

        cap_registered_managers_to_1_mock.assert_called_once()
        get_estimated_managers_diff_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.ESTIMATES_DESTINATION,
            partition_cols=job.partition_keys,
            append=False,
        )


def test_coalesce_ratios_with_source_label():
    expected_lf = pl.LazyFrame(
        {
            IndCQC.ascwds_job_role_ratios_filtered: [0.1, None, None],
            IndCQC.ascwds_job_role_ratios_interpolated: [0.1, 0.2, None],
            IndCQC.ascwds_job_role_rolling_ratio: [None, 0.2, 0.3],
            IndCQC.ascwds_job_role_ratios_merged: [0.1, 0.2, 0.3],
            IndCQC.ascwds_job_role_ratios_merged_source: [
                IndCQC.ascwds_job_role_ratios_filtered,
                IndCQC.ascwds_job_role_ratios_interpolated,
                IndCQC.ascwds_job_role_rolling_ratio,
            ],
        }
    )
    input_lf = expected_lf.drop(
        IndCQC.ascwds_job_role_ratios_merged,
        IndCQC.ascwds_job_role_ratios_merged_source,
    )
    returned_lf = job.coalesce_ratios_with_source_label(input_lf)
    pl_testing.assert_frame_equal(returned_lf, expected_lf)
