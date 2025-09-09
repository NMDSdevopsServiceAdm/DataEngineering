import shutil
import tempfile
import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    EstimateIndCQCFilledPostsByJobRoleData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    EstimateIndCQCFilledPostsByJobRoleSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.jobs.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_WORKER_SOURCE = "some/other/source"
    OUTPUT_DIR = "some/destination"

    original_environ = {}

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        self.test_estimated_ind_cqc_filled_posts_df = pl.DataFrame(
            Data.estimated_ind_cqc_filled_posts_rows,
            Schemas.estimated_ind_cqc_filled_posts_schema,
        )

        self.test_cleaned_ascwds_worker_df = pl.DataFrame(
            Data.cleaned_ascwds_worker_rows, Schemas.cleaned_ascwds_worker_schema
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_function(
        self,
        read_from_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.side_effect = [
            self.test_estimated_ind_cqc_filled_posts_df,
            self.test_cleaned_ascwds_worker_df,
        ]
        job.main(self.ESTIMATE_SOURCE, self.ASCWDS_WORKER_SOURCE, self.OUTPUT_DIR)

        self.assertEqual(read_from_parquet_mock.call_count, 2)
        read_from_parquet_mock.assert_has_calls(
            [
                call(
                    self.ESTIMATE_SOURCE,
                    selected_columns=job.estimated_ind_cqc_filled_posts_columns_to_import,
                ),
                call(
                    self.ASCWDS_WORKER_SOURCE,
                    selected_columns=job.cleaned_ascwds_worker_columns_to_import,
                ),
            ]
        )

        write_to_parquet_mock.assert_called_once_with(
            ANY, self.OUTPUT_DIR, "overwrite", PartitionKeys
        )
