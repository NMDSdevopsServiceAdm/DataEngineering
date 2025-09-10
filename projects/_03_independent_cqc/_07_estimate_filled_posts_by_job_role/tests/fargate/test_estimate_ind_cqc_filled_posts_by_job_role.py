import unittest
from unittest.mock import ANY, Mock, call, patch

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_WORKER_SOURCE = "some/other/source"
    OUTPUT_DIR = "some/destination"


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.pl.read_parquet")
    def test_main_runs(
        self,
        read_from_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        job.main(self.ESTIMATE_SOURCE, self.ASCWDS_WORKER_SOURCE, self.OUTPUT_DIR)

        self.assertEqual(read_from_parquet_mock.call_count, 2)
        read_from_parquet_mock.assert_has_calls(
            [
                call(
                    source=self.ESTIMATE_SOURCE,
                    columns=job.estimated_ind_cqc_filled_posts_columns_to_import,
                ),
                call(
                    source=self.ASCWDS_WORKER_SOURCE,
                    columns=job.cleaned_ascwds_worker_columns_to_import,
                ),
            ]
        )

        write_to_parquet_mock.assert_called_once_with(ANY, self.OUTPUT_DIR, logger=ANY)
