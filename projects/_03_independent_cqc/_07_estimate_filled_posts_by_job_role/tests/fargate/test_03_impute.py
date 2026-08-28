from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._03_impute as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._03_impute"

CLEANED_DATA_SOURCE = "some/source"
IMPUTED_DATA_DESTINATION = "some/destination"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_ascwds_job_role_counts")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_ascwds_job_role_ratios")
    @patch(f"{PATCH_PATH}.iUtils.create_ascwds_job_role_rolling_ratio")
    @patch(f"{PATCH_PATH}.iUtils.get_percent_share_ratios")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_each_step_reads_the_previous_step_output(
        self,
        scan_parquet_mock: Mock,
        get_percent_share_ratios_mock: Mock,
        create_ascwds_job_role_rolling_ratio_mock: Mock,
        add_imputed_ascwds_job_role_ratios_mock: Mock,
        add_imputed_ascwds_job_role_counts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(CLEANED_DATA_SOURCE, IMPUTED_DATA_DESTINATION)

        assert (
            get_percent_share_ratios_mock.call_args.args[0]
            is scan_parquet_mock.return_value
        )
        assert (
            create_ascwds_job_role_rolling_ratio_mock.call_args.args[0]
            is get_percent_share_ratios_mock.return_value
        )
        assert (
            add_imputed_ascwds_job_role_ratios_mock.call_args.args[0]
            is create_ascwds_job_role_rolling_ratio_mock.return_value
        )
        assert (
            add_imputed_ascwds_job_role_counts_mock.call_args.args[0]
            is add_imputed_ascwds_job_role_ratios_mock.return_value
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_ascwds_job_role_counts")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_ascwds_job_role_ratios")
    @patch(f"{PATCH_PATH}.iUtils.create_ascwds_job_role_rolling_ratio")
    @patch(f"{PATCH_PATH}.iUtils.get_percent_share_ratios")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_source_is_read_and_result_written_once(
        self,
        scan_parquet_mock: Mock,
        get_percent_share_ratios_mock: Mock,
        create_ascwds_job_role_rolling_ratio_mock: Mock,
        add_imputed_ascwds_job_role_ratios_mock: Mock,
        add_imputed_ascwds_job_role_counts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(CLEANED_DATA_SOURCE, IMPUTED_DATA_DESTINATION)

        scan_parquet_mock.assert_called_once_with(CLEANED_DATA_SOURCE)
        get_percent_share_ratios_mock.assert_called_once_with(
            ANY,
            input_col=IndCQC.ascwds_job_role_counts,
            output_col=IndCQC.ascwds_job_role_ratios,
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=add_imputed_ascwds_job_role_counts_mock.return_value,
            output_path=IMPUTED_DATA_DESTINATION,
        )


class TestNumericalValues:
    def test_extrapolation_period_value(self):
        assert job.NumericalValues.extrapolation_period == "2y"

    def test_interpolation_cap_period_value(self):
        assert job.NumericalValues.interpolation_cap_period == "5y"
