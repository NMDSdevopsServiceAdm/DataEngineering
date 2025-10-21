import shutil
import tempfile
import unittest
from glob import glob
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self):
        self.ESTIMATE_SOURCE = "some/source"
        self.PREPARED_JOB_ROLE_COUNTS_SOURCE = "some/other/source"
        self.TEMP_DIR = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.TEMP_DIR)


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.JRUtils.join_worker_to_estimates_dataframe")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_worker_to_estimates_dataframe_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.ESTIMATE_SOURCE, self.PREPARED_JOB_ROLE_COUNTS_SOURCE, self.TEMP_DIR
        )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    source=self.ESTIMATE_SOURCE,
                    selected_columns=job.estimated_ind_cqc_filled_posts_columns_to_import,
                ),
                call(
                    source=self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
                    selected_columns=job.prepared_ascwds_job_role_counts_columns_to_import,
                ),
            ]
        )

        join_worker_to_estimates_dataframe_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.TEMP_DIR,
            partition_cols=job.partition_keys,
            logger=job.logger,
            append=False,
        )
