from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_employment_status.fargate._03_impute as job

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._03_impute"


class TestMain:
    CLEANED_DATA_SOURCE = "some/source"
    IMPUTED_DATA_DESTINATION = "some/destination"

    def test_period_constants(self):
        assert job.EXTRAPOLATION_PERIOD == "2y"
        assert job.INTERPOLATION_CAP_PERIOD == "5y"
        assert job.ROLLING_AVERAGE_PERIOD == "6mo"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_pcts")
    @patch(f"{PATCH_PATH}.iUtils.add_rolling_avg_pcts")
    @patch(f"{PATCH_PATH}.iUtils.add_imputed_trendline_pcts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        add_imputed_trendline_pcts_mock: Mock,
        add_rolling_avg_pcts_mock: Mock,
        add_imputed_pcts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_DATA_SOURCE,
            self.IMPUTED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_DATA_SOURCE)
        add_imputed_trendline_pcts_mock.assert_called_once_with(
            scan_parquet_mock.return_value,
            extrapolation_period=job.EXTRAPOLATION_PERIOD,
            interpolation_cap_period=job.INTERPOLATION_CAP_PERIOD,
        )
        add_rolling_avg_pcts_mock.assert_called_once_with(
            add_imputed_trendline_pcts_mock.return_value,
            rolling_period=job.ROLLING_AVERAGE_PERIOD,
        )

        add_imputed_pcts_mock.assert_called_once_with(
            add_rolling_avg_pcts_mock.return_value
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=add_imputed_pcts_mock.return_value,
            output_path=self.IMPUTED_DATA_DESTINATION,
        )
