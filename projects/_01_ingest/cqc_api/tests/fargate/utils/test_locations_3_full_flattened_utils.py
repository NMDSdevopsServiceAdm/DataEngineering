import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import (
    locations_3_full_flattened_utils as job,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    FullFlattenUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    FullFlattenUtilsSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.utils.locations_3_full_flattened_utils"
)


class AllocateImportDatesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.delta_dates = [20250101, 20250201, 20250301]

    @patch(f"{PATCH_PATH}.utils.list_s3_parquet_import_dates")
    def test_allocate_import_dates_when_processed_list_populated(
        self, list_s3_mock: Mock
    ):
        processed_dates = [20250101]
        list_s3_mock.side_effect = [self.delta_dates, processed_dates]

        returned_dates_to_process, returned_processed_dates = job.allocate_import_dates(
            "s3://delta_path", "s3://full_path"
        )

        self.assertEqual(list_s3_mock.call_count, 2)

        self.assertEqual(returned_dates_to_process, [20250201, 20250301])
        self.assertEqual(returned_processed_dates, processed_dates)

    @patch(f"{PATCH_PATH}.utils.list_s3_parquet_import_dates")
    def test_allocate_import_dates_when_processed_list_empty(self, list_s3_mock: Mock):
        processed_dates = []
        list_s3_mock.side_effect = [self.delta_dates, processed_dates]

        returned_dates_to_process, returned_processed_dates = job.allocate_import_dates(
            "s3://delta_path", "s3://full_path"
        )

        self.assertEqual(list_s3_mock.call_count, 2)

        self.assertEqual(returned_dates_to_process, self.delta_dates)
        self.assertEqual(returned_processed_dates, processed_dates)


class LoadLatestSnapshotTests(unittest.TestCase):
    def test_load_latest_snapshot_when_no_existing_snapshots(self):
        result = job.load_latest_snapshot("s3://some_path", [])
        self.assertIsNone(result)

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_load_latest_snapshot_returns_latest_lf(self, scan_parquet_mock: Mock):
        lf = pl.LazyFrame(
            data=Data.load_latest_snapshot,
            schema=Schemas.load_latest_snapshot_schema,
        )
        scan_parquet_mock.return_value = lf

        returned_lf = job.load_latest_snapshot(
            "s3://some_path", Data.load_latest_snapshot_existing_dates
        )

        expected_lf = pl.LazyFrame(
            data=Data.expected_load_latest_snapshot,
            schema=Schemas.load_latest_snapshot_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CreateFullSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.full_lf = pl.LazyFrame(
            data=Data.create_full_snapshot_full_lf,
            schema=Schemas.create_full_snapshot_schema,
        )
        self.delta_lf = pl.LazyFrame(
            data=Data.create_full_snapshot_delta_lf,
            schema=Schemas.create_full_snapshot_schema,
        )
        self.primary_key = CQCLClean.location_id

    def test_returns_delta_lf_when_full_lf_is_none(self):
        returned_lf = job.create_full_snapshot(None, self.delta_lf, self.primary_key)

        pl_testing.assert_frame_equal(returned_lf, self.delta_lf)

    def test_merges_lfs_and_retains_latest_data_for_each_location(self):
        returned_lf = job.create_full_snapshot(
            self.full_lf, self.delta_lf, self.primary_key
        )

        expected_lf = pl.LazyFrame(
            data=Data.expected_create_full_snapshot_lf,
            schema=Schemas.create_full_snapshot_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(self.primary_key),
            expected_lf.sort(self.primary_key),
        )


class ApplyPartitionsTests(unittest.TestCase):
    def test_apply_partitions_when_import_date_is_int(self):
        lf = pl.LazyFrame(
            data=Data.apply_partitions,
            schema=Schemas.apply_partitions_schema,
        )

        returned_lf = job.apply_partitions(lf, Data.apply_partitions_import_date_int)

        expected_lf = pl.LazyFrame(
            data=Data.expected_apply_partitions,
            schema=Schemas.apply_partitions_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_apply_partitions_when_import_date_is_str(self):
        lf = pl.LazyFrame(
            data=Data.apply_partitions,
            schema=Schemas.apply_partitions_schema,
        )

        returned_lf = job.apply_partitions(lf, Data.apply_partitions_import_date_str)

        expected_lf = pl.LazyFrame(
            data=Data.expected_apply_partitions,
            schema=Schemas.apply_partitions_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
