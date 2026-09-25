from unittest.mock import Mock, patch

import pytest

import projects._03_independent_cqc._02_employment_status.fargate._03_impute_spike_prototype as job

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._03_impute_spike_prototype"

CLEANED_DATA_SOURCE = "s3://some-datasets/some/source"
SPIKE_DATA_DESTINATION = "s3://some-datasets/some/spike_destination"
DIAGNOSTICS_JOB_NAME = "empstat_spike_l2_red"


class TestMain:
    @patch(f"{PATCH_PATH}.RunDiagnostics")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_employment_status_rates")
    @patch(f"{PATCH_PATH}.iUtils.add_rolling_employment_status_ratios")
    @patch(f"{PATCH_PATH}.fUtils.filter_out_zero_job_role_filled_posts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_applies_zero_filter_before_imputation(
        self,
        scan_parquet_mock: Mock,
        filter_mock: Mock,
        add_rolling_mock: Mock,
        add_imputed_mock: Mock,
        sink_to_parquet_mock: Mock,
        run_diagnostics_mock: Mock,
    ):
        run_diagnostics_mock.return_value.start.return_value = (
            run_diagnostics_mock.return_value
        )

        job.main(CLEANED_DATA_SOURCE, SPIKE_DATA_DESTINATION, DIAGNOSTICS_JOB_NAME)

        scan_parquet_mock.assert_called_once_with(
            CLEANED_DATA_SOURCE, selected_columns=job.fUtils.SPIKE_INPUT_COLUMNS
        )
        filter_mock.assert_called_once_with(scan_parquet_mock.return_value)
        add_rolling_mock.assert_called_once_with(
            filter_mock.return_value,
            extrapolation_period=job.EXTRAPOLATION_PERIOD,
            interpolation_cap_period=job.INTERPOLATION_CAP_PERIOD,
        )
        add_imputed_mock.assert_called_once_with(add_rolling_mock.return_value)

    @patch(f"{PATCH_PATH}.RunDiagnostics")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_employment_status_rates")
    @patch(f"{PATCH_PATH}.iUtils.add_rolling_employment_status_ratios")
    @patch(f"{PATCH_PATH}.fUtils.filter_out_zero_job_role_filled_posts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_writes_to_spike_output_dataset(
        self,
        scan_parquet_mock: Mock,
        filter_mock: Mock,
        add_rolling_mock: Mock,
        add_imputed_mock: Mock,
        sink_to_parquet_mock: Mock,
        run_diagnostics_mock: Mock,
    ):
        run_diagnostics_mock.return_value.start.return_value = (
            run_diagnostics_mock.return_value
        )

        job.main(CLEANED_DATA_SOURCE, SPIKE_DATA_DESTINATION, DIAGNOSTICS_JOB_NAME)

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=add_imputed_mock.return_value,
            output_path=SPIKE_DATA_DESTINATION,
        )
        run_diagnostics_mock.assert_called_once_with(
            DIAGNOSTICS_JOB_NAME, "some-datasets"
        )

    @patch(f"{PATCH_PATH}.RunDiagnostics")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_employment_status_rates")
    @patch(f"{PATCH_PATH}.iUtils.add_rolling_employment_status_ratios")
    @patch(f"{PATCH_PATH}.fUtils.filter_out_zero_job_role_filled_posts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_diagnostics_stop_called_even_if_imputation_raises(
        self,
        scan_parquet_mock: Mock,
        filter_mock: Mock,
        add_rolling_mock: Mock,
        add_imputed_mock: Mock,
        sink_to_parquet_mock: Mock,
        run_diagnostics_mock: Mock,
    ):
        run_diagnostics_mock.return_value.start.return_value = (
            run_diagnostics_mock.return_value
        )
        add_imputed_mock.side_effect = ValueError("imputation failed")

        with pytest.raises(ValueError):
            job.main(CLEANED_DATA_SOURCE, SPIKE_DATA_DESTINATION, DIAGNOSTICS_JOB_NAME)

        run_diagnostics_mock.return_value.stop.assert_called_once()
        sink_to_parquet_mock.assert_not_called()
