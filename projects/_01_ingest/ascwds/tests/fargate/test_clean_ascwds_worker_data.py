from unittest.mock import ANY, Mock, call, patch

import projects._01_ingest.ascwds.fargate.clean_ascwds_worker_data as job

PATCH_PATH = "projects._01_ingest.ascwds.fargate.clean_ascwds_worker_data"


class TestMain:
    WORKER_SOURCE = "some/worker/source"
    CLEANED_WORKPLACE_SOURCE = "some/workplace/source"
    DATA_LABELS_SOURCE = "some/labels/source"
    CLEANED_WORKER_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.wUtils.create_clean_employment_status_column")
    @patch(f"{PATCH_PATH}.wUtils.create_clean_main_job_role_column")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.wUtils.remove_workers_without_workplaces")
    @patch(f"{PATCH_PATH}.is_unique_worker_data")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        is_unique_worker_data_mock: Mock,
        remove_workers_without_workplaces_mock: Mock,
        scan_csv_mock: Mock,
        create_clean_main_job_role_column_mock: Mock,
        create_clean_employment_status_column_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.WORKER_SOURCE,
            self.CLEANED_WORKPLACE_SOURCE,
            self.DATA_LABELS_SOURCE,
            self.CLEANED_WORKER_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 2
        assert scan_parquet_mock.call_args_list[0] == call(
            self.WORKER_SOURCE, schema=job.WORKER_SCHEMA
        )
        assert scan_parquet_mock.call_args_list[1] == call(
            self.CLEANED_WORKPLACE_SOURCE, schema=job.WORKPLACE_SCHEMA
        )

        column_to_date_mock.assert_called_once()
        is_unique_worker_data_mock.assert_called_once()
        remove_workers_without_workplaces_mock.assert_called_once()

        scan_csv_mock.assert_called_once_with(
            self.DATA_LABELS_SOURCE, schema=job.data_labels_schema
        )
        create_clean_main_job_role_column_mock.assert_called_once()
        create_clean_employment_status_column_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            ANY, output_path=self.CLEANED_WORKER_DESTINATION
        )
