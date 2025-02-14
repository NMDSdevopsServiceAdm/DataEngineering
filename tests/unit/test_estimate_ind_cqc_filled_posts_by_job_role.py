import unittest
from unittest.mock import ANY, call, patch, Mock

import jobs.estimate_ind_cqc_filled_posts_by_job_role as job
from tests.test_file_data import EstimateIndCQCFilledPostsByJobRoleData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsByJobRoleSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_WORKER_SOURCE = "some/other/source"
    OUTPUT_DIR = "some/destination"

    def setUp(self):
        self.spark = utils.get_spark()

        self.test_estimated_ind_cqc_filled_posts_df = self.spark.createDataFrame(
            Data.estimated_ind_cqc_filled_posts_rows,
            Schemas.estimated_ind_cqc_filled_posts_schema,
        )
        self.test_cleaned_ascwds_worker_df = self.spark.createDataFrame(
            Data.cleaned_ascwds_worker_rows, Schemas.cleaned_ascwds_worker_schema
        )


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch("utils.utils.write_to_parquet")
    @patch(
        "jobs.estimate_ind_cqc_filled_posts_by_job_role.count_job_role_per_establishment_as_columns"
    )
    @patch("utils.utils.read_from_parquet")
    def test_main_function(
        self,
        read_from_parquet_mock: Mock,
        count_job_role_per_establishment_as_columns_mock: Mock,
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

        self.assertEqual(count_job_role_per_establishment_as_columns_mock.call_count, 1)

        write_to_parquet_mock.assert_called_once_with(
            ANY, self.OUTPUT_DIR, "overwrite", PartitionKeys
        )
