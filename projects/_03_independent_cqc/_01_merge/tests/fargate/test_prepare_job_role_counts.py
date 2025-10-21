import shutil
import tempfile
import unittest
from glob import glob
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._01_merge.fargate.prepare_job_role_counts as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.prepare_job_role_counts"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self):
        self.CLEANED_ASCWDS_WORKER_SOURCE = "some/source"
        self.TEMP_DIR = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.TEMP_DIR)


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.JRUtils.aggregate_ascwds_worker_job_roles_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        aggregate_ascwds_worker_job_roles_per_establishment_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKER_SOURCE,
            self.TEMP_DIR,
        )

        scan_parquet_mock.assert_called_once()
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    source=self.CLEANED_ASCWDS_WORKER_SOURCE,
                    selected_columns=job.cleaned_ascwds_worker_columns_to_import,
                ),
            ]
        )
        aggregate_ascwds_worker_job_roles_per_establishment_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.TEMP_DIR,
            partition_cols=job.partition_keys,
            logger=job.logger,
            append=False,
        )
