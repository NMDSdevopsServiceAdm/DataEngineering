from unittest.mock import Mock, patch

import projects._98_hackathon.fargate._01_worker_turnover as job

PATCH_PATH = "projects._98_hackathon.fargate._01_worker_turnover"


class TestMain:
    CLEANED_WORKER_SOURCE = "some/worker/source"
    FLAGGED_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.wUtils.filter_worker_turnover_data")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        filter_worker_turnover_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.CLEANED_WORKER_SOURCE, self.FLAGGED_DESTINATION)

        scan_parquet_mock.assert_called_once_with(self.CLEANED_WORKER_SOURCE)
        filter_worker_turnover_data_mock.assert_called_once_with(
            scan_parquet_mock.return_value
        )
        sink_to_parquet_mock.assert_called_once_with(
            filter_worker_turnover_data_mock.return_value,
            output_path=self.FLAGGED_DESTINATION,
        )
