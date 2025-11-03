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


class GetImportDatesToProcessTests(unittest.TestCase):
    def test_get_import_dates_to_process(self):
        lf = pl.LazyFrame(
            data=Data.get_import_dates_to_process,
            schema=Schemas.get_import_dates_to_process_schema,
        )

        returned_list = job.get_import_dates_to_process(
            lf, Data.get_import_dates_existing
        )

        self.assertEqual(returned_list, Data.expected_import_dates_to_process_list)


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

    def test_returns_delta_lf_when_full_lf_is_none(self):
        returned_lf = job.create_full_snapshot(None, self.delta_lf)

        pl_testing.assert_frame_equal(returned_lf, self.delta_lf)

    def test_merges_lfs_and_retains_latest_data_for_each_location(self):
        returned_lf = job.create_full_snapshot(self.full_lf, self.delta_lf)

        expected_lf = pl.LazyFrame(
            data=Data.expected_create_full_snapshot_lf,
            schema=Schemas.create_full_snapshot_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(CQCLClean.location_id),
            expected_lf.sort(CQCLClean.location_id),
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
