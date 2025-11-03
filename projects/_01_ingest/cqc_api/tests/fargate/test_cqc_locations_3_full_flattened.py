import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.fargate.cqc_locations_3_full_flattened as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.cqc_locations_3_full_flattened"


class CqcLocationsFullFlattenTests(unittest.TestCase):
    TEST_SOURCE = "s3://some/source"
    TEST_DEST = "s3://some/dest"
    PARTITION_KEYS = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.fUtils.apply_partitions")
    @patch(f"{PATCH_PATH}.fUtils.create_full_snapshot")
    @patch(f"{PATCH_PATH}.fUtils.load_latest_snapshot")
    @patch(f"{PATCH_PATH}.fUtils.get_import_dates_to_process")
    @patch(f"{PATCH_PATH}.utils.list_s3_parquet_import_dates")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_no_new_import_dates(
        self,
        scan_parquet_mock: Mock,
        list_s3_mock: Mock,
        get_dates_mock: Mock,
        load_snapshot_mock: Mock,
        create_full_mock: Mock,
        apply_partitions_mock: Mock,
        sink_mock: Mock,
    ):
        # Simulate no new import_dates
        list_s3_mock.return_value = [20231001]
        get_dates_mock.return_value = []
        load_snapshot_mock.return_value = Mock(name="full_lf")
        create_full_mock.side_effect = lambda full, delta: delta
        apply_partitions_mock.side_effect = lambda lf, date: lf

        job.main(self.TEST_SOURCE, self.TEST_DEST)

        scan_parquet_mock.assert_called_once()
        list_s3_mock.assert_called_once()
        get_dates_mock.assert_called_once()
        load_snapshot_mock.assert_not_called()
        create_full_mock.assert_not_called()
        apply_partitions_mock.assert_not_called()
        sink_mock.assert_not_called()

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.fUtils.apply_partitions")
    @patch(f"{PATCH_PATH}.fUtils.create_full_snapshot")
    @patch(f"{PATCH_PATH}.fUtils.load_latest_snapshot")
    @patch(f"{PATCH_PATH}.fUtils.get_import_dates_to_process")
    @patch(f"{PATCH_PATH}.utils.list_s3_parquet_import_dates")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_first_snapshot(
        self,
        scan_parquet_mock: Mock,
        list_s3_mock: Mock,
        get_dates_mock: Mock,
        load_snapshot_mock: Mock,
        create_full_mock: Mock,
        apply_partitions_mock: Mock,
        sink_mock: Mock,
    ):
        # Destination empty: first delta becomes full snapshot
        scan_parquet_mock.return_value = Mock(name="delta_lf")
        list_s3_mock.return_value = []
        get_dates_mock.return_value = [20231001]
        load_snapshot_mock.return_value = Mock(name="full_lf")
        create_full_mock.side_effect = lambda full, delta: delta
        apply_partitions_mock.side_effect = lambda lf, date: lf

        job.main(self.TEST_SOURCE, self.TEST_DEST)

        scan_parquet_mock.assert_called_once()
        list_s3_mock.assert_called_once()
        get_dates_mock.assert_called_once()
        load_snapshot_mock.assert_called_once()
        create_full_mock.assert_called_once()
        apply_partitions_mock.assert_called_once()
        sink_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.fUtils.apply_partitions")
    @patch(f"{PATCH_PATH}.fUtils.create_full_snapshot")
    @patch(f"{PATCH_PATH}.fUtils.load_latest_snapshot")
    @patch(f"{PATCH_PATH}.fUtils.get_import_dates_to_process")
    @patch(f"{PATCH_PATH}.utils.list_s3_parquet_import_dates")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_multiple_new_import_dates(
        self,
        scan_parquet_mock: Mock,
        list_s3_mock: Mock,
        get_dates_mock: Mock,
        load_snapshot_mock: Mock,
        create_full_mock: Mock,
        apply_partitions_mock: Mock,
        sink_mock: Mock,
    ):
        scan_parquet_mock.return_value = Mock(name="delta_lf")
        list_s3_mock.return_value = [20231001]
        get_dates_mock.return_value = [20231002, 20231003]
        load_snapshot_mock.return_value = Mock(name="full_lf")
        apply_partitions_mock.side_effect = lambda lf, date: lf

        # simulate merging
        create_full_mock.side_effect = lambda full, delta: Mock(name=f"merged_{delta}")

        job.main(self.TEST_SOURCE, self.TEST_DEST)

        scan_parquet_mock.assert_called_once()
        list_s3_mock.assert_called_once()
        get_dates_mock.assert_called_once()
        load_snapshot_mock.assert_called_once()
        self.assertEqual(create_full_mock.call_count, 2)
        self.assertEqual(apply_partitions_mock.call_count, 2)
        self.assertEqual(sink_mock.call_count, 2)
