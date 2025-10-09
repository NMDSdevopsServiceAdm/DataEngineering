import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateIndCQCFilledPostsByJobRoleUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateIndCQCFilledPostsByJobRoleUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self):
        self.ESTIMATE_SOURCE = "some/source"
        self.ASCWDS_WORKER_SOURCE = "some/other/source"
        self.OUTPUT_DIR = "some/destination/"
        self.OUTPUT_FILE_NAME = "file.parquet"


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.JRUtils.join_worker_to_estimates_dataframe")
    @patch(f"{PATCH_PATH}.JRUtils.aggregate_ascwds_worker_job_roles_per_establishment")
    @patch(f"{PATCH_PATH}.pl.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_worker_to_estimates_dataframe_mock: Mock,
        aggregate_ascwds_worker_job_roles_per_establishment_mock: Mock,
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

        aggregate_ascwds_worker_job_roles_per_establishment_mock.assert_called_once()
        join_worker_to_estimates_dataframe_mock.assert_called_once()

        write_to_parquet_mock.assert_called_once_with(
            df=ANY,
            output_path=self.OUTPUT_DIR + self.OUTPUT_FILE_NAME,
            logger=ANY,
            append=False,
        )
