import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._01_merge.fargate.prepare_job_role_counts as job

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.prepare_job_role_counts"


class MainTests(unittest.TestCase):
    CLEANED_ASCWDS_WORKER_SOURCE = "some/source"
    PREPARED_JOB_ROLE_COUNTS_DESTINATION = "some/destination"

    mock_ascwds_worker_data = Mock(name="ascwds_worker_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.JRUtils.aggregate_ascwds_worker_job_roles_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_ascwds_worker_data)
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        aggregate_ascwds_worker_job_roles_per_establishment_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKER_SOURCE,
            self.PREPARED_JOB_ROLE_COUNTS_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            source=self.CLEANED_ASCWDS_WORKER_SOURCE,
            selected_columns=job.cleaned_ascwds_worker_columns_to_import,
        )

        aggregate_ascwds_worker_job_roles_per_establishment_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.PREPARED_JOB_ROLE_COUNTS_DESTINATION,
            partition_cols=job.partition_keys,
            logger=job.logger,
            append=False,
        )
