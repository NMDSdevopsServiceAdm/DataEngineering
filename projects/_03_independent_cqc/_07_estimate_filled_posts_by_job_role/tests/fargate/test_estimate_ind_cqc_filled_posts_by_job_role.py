import unittest
from unittest.mock import ANY, Mock, call, patch

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_WORKER_SOURCE = "some/other/source"
    OUTPUT_DIR = "some/destination"


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        estimated_ind_cqc_filled_posts_scan_mock = Mock()
        cleaned_ascwds_worker_scan_mock = Mock()
        scan_parquet_mock.side_effect = [
            estimated_ind_cqc_filled_posts_scan_mock,
            cleaned_ascwds_worker_scan_mock,
        ]

        job.main(self.ESTIMATE_SOURCE, self.ASCWDS_WORKER_SOURCE, self.OUTPUT_DIR)

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    self.ESTIMATE_SOURCE,
                ),
                call(
                    self.ASCWDS_WORKER_SOURCE,
                ),
            ]
        )
        estimated_ind_cqc_filled_posts_scan_mock.select.assert_called_once_with(
            job.estimated_ind_cqc_filled_posts_columns_to_import
        )
        cleaned_ascwds_worker_scan_mock.select.assert_called_once_with(
            job.cleaned_ascwds_worker_columns_to_import
        )

        write_to_parquet_mock.assert_called_once_with(ANY, self.OUTPUT_DIR, logger=ANY)
