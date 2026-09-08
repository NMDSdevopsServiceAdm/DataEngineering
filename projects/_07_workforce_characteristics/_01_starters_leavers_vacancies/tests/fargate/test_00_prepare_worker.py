from unittest.mock import Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare_worker as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare_worker"


class TestPrepareWorker:
    CLEANED_ASCWDS_WORKER_SOURCE = "some/source"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pWorkerUtils.reshape_employment_status_data")
    @patch(f"{PATCH_PATH}.pWorkerUtils.aggregate_employment_status_data")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        aggregate_employment_status_data_mock: Mock,
        reshape_employment_status_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKER_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_ASCWDS_WORKER_SOURCE)
        worker_lf = scan_parquet_mock.return_value

        aggregate_employment_status_data_mock.assert_called_once_with(worker_lf)
        aggregated_lf = aggregate_employment_status_data_mock.return_value

        reshape_employment_status_data_mock.assert_called_once_with(aggregated_lf)
        reshaped_lf = reshape_employment_status_data_mock.return_value

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=reshaped_lf,
            output_path=self.PREPARED_DATA_DESTINATION,
        )
