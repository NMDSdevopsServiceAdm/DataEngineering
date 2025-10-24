import unittest
from unittest.mock import ANY, Mock, patch

import projects._01_ingest.cqc_api.fargate.cqc_locations_2_flatten as job
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.cqc_locations_2_flatten"


class CqcLocationsFlattenTests(unittest.TestCase):
    TEST_SOURCE = "some/source"
    TEST_DESTINATION = "some/destination"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_cqc_locations_data = Mock(name="cqc_locations_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.fUtils.impute_missing_struct_columns")
    @patch(f"{PATCH_PATH}.fUtils.assign_cqc_sector")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_cqc_locations_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        assign_cqc_sector_mock: Mock,
        impute_missing_struct_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(
            self.TEST_SOURCE, schema=ANY, selected_columns=ANY
        )
        assign_cqc_sector_mock.assert_called_once()
        impute_missing_struct_columns_mock.assert_called_once_with(
            ANY,
            [
                CQCLClean.gac_service_types,
                CQCLClean.regulated_activities,
                CQCLClean.specialisms,
            ],
        )
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            logger=ANY,
            partition_cols=self.partition_keys,
            append=False,
        )
