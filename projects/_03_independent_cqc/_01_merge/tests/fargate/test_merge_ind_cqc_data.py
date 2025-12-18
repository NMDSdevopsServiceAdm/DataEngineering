import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._01_merge.fargate.merge_ind_cqc_data as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    MergeIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    MergeIndCQCSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.merge_ind_cqc_data"


class IndCQCMergeTests(unittest.TestCase):
    TEST_SOURCE = "some/source"
    TEST_DESTINATION = "some/destination"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            partition_cols=self.partition_keys,
            append=False,
        )
