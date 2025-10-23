import unittest
from unittest.mock import ANY, Mock, call, patch

import projects._01_ingest.cqc_api.fargate.cqc_locations_4_clean as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.cqc_locations_4_clean"


class CqcLocationsFlattenTests(unittest.TestCase):
    TEST_CQC_SOURCE = "some/cqc/source"
    TEST_ONS_SOURCE = "some/ons/source"
    TEST_REG_DESTINATION = "some/reg/destination"
    TEST_DEREG_DESTINATION = "some/reg/destination"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_cqc_locations_data = Mock(name="cqc_locations_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_cqc_locations_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.TEST_CQC_SOURCE,
            self.TEST_ONS_SOURCE,
            self.TEST_REG_DESTINATION,
            self.TEST_DEREG_DESTINATION,
        )

        scan_parquet_mock.assert_has_calls(
            [
                call(self.TEST_CQC_SOURCE),
                call(self.TEST_ONS_SOURCE, selected_columns=ANY),
            ]
        )
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_REG_DESTINATION,
            logger=ANY,
            partition_cols=self.partition_keys,
            append=False,
        )
