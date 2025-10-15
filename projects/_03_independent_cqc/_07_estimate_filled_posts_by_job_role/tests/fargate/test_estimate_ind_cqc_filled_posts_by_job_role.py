import os
import shutil
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self):
        self.ESTIMATE_SOURCE = "some/source"
        self.ASCWDS_WORKER_SOURCE = "some/other/source"
        self.OUTPUT_DIR = "some/destination"
        self.OUTPUT_FILE_NAME = "estimated_ind_cqc_filled_posts_by_job_role_lf.parquet"

    def tearDown(self):
        if os.path.exists(
            "some\destination\year=2025\month=01\day=01\import_date=20250101"
        ):
            shutil.rmtree(
                "some\destination\year=2025\month=01\day=01\import_date=20250101"
            )


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.sink_parquet_with_partitions")
    @patch(f"{PATCH_PATH}.JRUtils.join_worker_to_estimates_dataframe")
    @patch(f"{PATCH_PATH}.JRUtils.aggregate_ascwds_worker_job_roles_per_establishment")
    @patch(f"{PATCH_PATH}.pl.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_worker_to_estimates_dataframe_mock: Mock,
        aggregate_ascwds_worker_job_roles_per_establishment_mock: Mock,
        sink_parquet_with_partitions_mock: Mock,
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

        sink_parquet_with_partitions_mock.assert_called_once_with(ANY, self.OUTPUT_DIR)


class SinkParquetWithPartitions(EstimateIndCQCFilledPostsByJobRoleTests):
    def test_sink_parquet_with_partitions(
        self,
    ):
        test_lf = pl.LazyFrame(
            {
                Keys.year: "2025",
                Keys.month: "01",
                Keys.day: "01",
                Keys.import_date: "20250101",
            }
        )

        job.sink_parquet_with_partitions(test_lf, self.OUTPUT_DIR)

        self.assertTrue(Path(self.OUTPUT_DIR).is_dir())
        self.assertTrue(
            Path(
                "some\destination\year=2025\month=01\day=01\import_date=20250101"
            ).exists()
        )
