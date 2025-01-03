import unittest
from unittest.mock import ANY, call, patch, Mock

import jobs.estimate_ind_cqc_filled_posts_by_job_role as job
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self) -> None:
        pass


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_function(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        ESTIMATE_SOURCE = "some/source"
        ASCWDS_WORKER_SOURCE = "some/other/source"
        OUTPUT_DIR = "some/destination"

        job.main(ESTIMATE_SOURCE, ASCWDS_WORKER_SOURCE, OUTPUT_DIR)

        read_from_parquet_mock.assert_has_calls(
            [
                call(
                    ESTIMATE_SOURCE,
                    selected_columns=job.estimated_ind_cqc_filled_posts_columns_to_import,
                ),
                call(
                    ASCWDS_WORKER_SOURCE,
                    selected_columns=job.cleaned_ascwds_worker_columns_to_import,
                ),
            ]
        )
        write_to_parquet_mock.assert_called_once_with(
            ANY, OUTPUT_DIR, "overwrite", PartitionKeys
        )
