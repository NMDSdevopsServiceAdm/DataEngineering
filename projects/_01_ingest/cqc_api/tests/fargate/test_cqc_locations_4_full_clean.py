import unittest
from unittest.mock import ANY, Mock, call, patch

import projects._01_ingest.cqc_api.fargate.cqc_locations_4_full_clean as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.cqc_locations_4_full_clean"


class CqcLocationsFullCleanTests(unittest.TestCase):
    TEST_CQC_SOURCE = "some/cqc/source"
    TEST_ONS_SOURCE = "some/ons/source"
    TEST_REG_DESTINATION = "some/reg/destination"
    TEST_DEREG_DESTINATION = "some/reg/destination"
    TEST_MANUAL_POSTCODE_CORRETIONS_SOURCE = "some/ons/source"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_cqc_locations_data = Mock(name="cqc_locations_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pmUtils.run_postcode_matching")
    @patch(f"{PATCH_PATH}.cUtils.classify_specialisms")
    @patch(f"{PATCH_PATH}.cUtils.add_related_location_column")
    @patch(f"{PATCH_PATH}.cUtils.realign_carehome_column_with_primary_service")
    @patch(f"{PATCH_PATH}.cUtils.allocate_primary_service_type_second_level")
    @patch(f"{PATCH_PATH}.cUtils.allocate_primary_service_type")
    @patch(f"{PATCH_PATH}.cUtils.remove_specialist_colleges")
    @patch(f"{PATCH_PATH}.cUtils.assign_cqc_sector")
    @patch(f"{PATCH_PATH}.cUtils.clean_provider_id_column")
    @patch(f"{PATCH_PATH}.cUtils.clean_and_impute_registration_date")
    @patch(f"{PATCH_PATH}.cUtils.save_latest_full_snapshot")
    @patch(f"{PATCH_PATH}.column_to_date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_cqc_locations_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        column_to_date_mock: Mock,
        save_latest_full_snapshot_mock: Mock,
        clean_and_impute_registration_date_mock: Mock,
        clean_provider_id_column_mock: Mock,
        assign_cqc_sector_mock: Mock,
        remove_specialist_colleges_mock: Mock,
        allocate_primary_service_type_mock: Mock,
        allocate_primary_service_type_second_level_mock: Mock,
        realign_carehome_column_with_primary_service_mock: Mock,
        add_related_location_column_mock: Mock,
        classify_specialisms_mock: Mock,
        run_postcode_matching_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.TEST_CQC_SOURCE,
            self.TEST_ONS_SOURCE,
            self.TEST_REG_DESTINATION,
            self.TEST_DEREG_DESTINATION,
            self.TEST_MANUAL_POSTCODE_CORRETIONS_SOURCE,
        )

        scan_parquet_mock.assert_has_calls(
            [
                call(self.TEST_CQC_SOURCE),
                call(self.TEST_ONS_SOURCE, selected_columns=ANY),
            ]
        )
        column_to_date_mock.assert_called_once()
        save_latest_full_snapshot_mock.assert_called_once()
        clean_and_impute_registration_date_mock.assert_called_once()
        clean_provider_id_column_mock.assert_called_once()
        assign_cqc_sector_mock.assert_called_once()
        remove_specialist_colleges_mock.assert_called_once()
        allocate_primary_service_type_mock.assert_called_once()
        allocate_primary_service_type_second_level_mock.assert_called_once()
        realign_carehome_column_with_primary_service_mock.assert_called_once()
        add_related_location_column_mock.assert_called_once()
        classify_specialisms_mock.assert_called_once()
        run_postcode_matching_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_REG_DESTINATION,
            logger=ANY,
            partition_cols=self.partition_keys,
            append=False,
        )
