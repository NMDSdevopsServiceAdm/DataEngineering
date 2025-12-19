import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._01_merge.fargate.merge_ind_cqc_data as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.merge_ind_cqc_data"


class IndCQCMergeTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_CQC_PIR_SOURCE = "some/other/directory"
    TEST_ASCWDS_WORKPLACE_SOURCE = "some/other/other/directory"
    TEST_CT_NON_RES_SOURCE = "yet/another/directory"
    TEST_CT_CARE_HOME_SOURCE = "one/more/directory"
    TEST_DESTINATION = "an/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.join_data_into_cqc_lf")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        join_data_into_cqc_lf_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_CQC_PIR_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_CT_NON_RES_SOURCE,
            self.TEST_CT_CARE_HOME_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 5)
        # self.assertEqual(join_data_into_cqc_lf_mock.call_count, 4)
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            partition_cols=self.partition_keys,
            append=False,
        )
