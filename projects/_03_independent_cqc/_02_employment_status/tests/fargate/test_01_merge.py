from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._02_employment_status.fargate._01_merge as job

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._01_merge"


class TestMain:
    METADATA_SOURCE = "some/source"
    JOB_ROLE_ESTIMATES_SOURCE = "another/source"
    PREPARED_WORKER_SOURCE = "worker/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.collapse_job_role_estimates_to_published_labels")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        collapse_job_role_estimates_to_published_labels_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.PREPARED_WORKER_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        assert len(scan_parquet_mock.call_args_list) == 3

        scan_parquet_mock.assert_any_call(
            source=self.METADATA_SOURCE, selected_columns=job.metadata_columns
        )
        scan_parquet_mock.assert_any_call(
            source=self.JOB_ROLE_ESTIMATES_SOURCE,
            selected_columns=job.job_role_estimates_columns,
        )
        scan_parquet_mock.assert_any_call(
            self.PREPARED_WORKER_SOURCE, selected_columns=job.worker_columns
        )

        collapse_job_role_estimates_to_published_labels_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.MERGED_DATA_DESTINATION,
        )
