import unittest
from unittest.mock import patch, Mock

import polars as pl
import polars.testing as pl_testing

import lambdas.utils.snapshots as job


PATCH_PATH = "lambdas.utils.snapshots"


class UtilsTests(unittest.TestCase):
    def setUp(self):
        self.base_snapshot = pl.DataFrame(
            {
                "import_date": [
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                ],
                "providerId": ["a", "b", "c", "d", "e"],
                "value": ["same", "same", "same", "same", "same"],
                "deregistrationDate": ["", "", "", "", ""],
            }
        )

        self.first_delta = pl.DataFrame(
            {
                "import_date": [
                    20130401,
                    20130401,
                    20130401,
                    20130401,
                ],
                "providerId": ["b", "d", "f", "e"],
                "value": [
                    "different",
                    "different",
                    "new",
                    "same",
                ],
                "deregistrationDate": ["", "", "", "20130401"],
            }
        )

        self.second_full_snapshot = pl.DataFrame(
            {
                "import_date": [
                    20130401,
                    20130401,
                    20130401,
                    20130401,
                    20130401,
                    20130401,
                ],
                "providerId": ["a", "b", "c", "d", "e", "f"],
                "value": ["same", "different", "same", "different", "same", "new"],
                "deregistrationDate": ["", "", "", "", "20130401", ""],
            }
        )

    @patch(f"{PATCH_PATH}.pl.read_parquet")
    def test_snapshots(self, mock_read_parquet: Mock):
        # Given
        mock_read_parquet.return_value = self.base_snapshot

        expected = self.base_snapshot

        # When
        generator = job.get_snapshots(
            "bucket", "read_folder", organisation_type="providers"
        )

        # Then
        pl_testing.assert_frame_equal(next(generator), expected)
        with self.assertRaises(StopIteration):
            next(generator)

        mock_read_parquet.assert_called_once()

    @patch(f"{PATCH_PATH}.pl.read_parquet")
    def test_snapshots_multiple_timepoints(self, mock_read_parquet: Mock):
        # Given
        mock_read_parquet.return_value = pl.concat(
            [self.base_snapshot, self.first_delta]
        )

        expected_first = self.base_snapshot
        expected_second = self.second_full_snapshot

        # When
        generator = job.get_snapshots(
            "bucket", "read_folder", organisation_type="providers"
        )

        # Then
        pl_testing.assert_frame_equal(
            next(generator), expected_first, check_row_order=False
        )
        pl_testing.assert_frame_equal(
            next(generator).drop(["year", "month", "day"]),
            expected_second,
            check_row_order=False,
        )
        with self.assertRaises(StopIteration):
            next(generator)

        mock_read_parquet.assert_called_once()

    @patch(f"{PATCH_PATH}.get_snapshots")
    def test_build_snapshot_table_from_delta(self, mock_snapshots: Mock):
        # Given
        mock_snapshots.return_value = [
            self.base_snapshot,
        ]

        expected = self.base_snapshot

        # When
        result = job.build_snapshot_table_from_delta(
            "bucket", "read_folder", organisation_type="providers", timepoint=20130301
        )

        # Then
        pl_testing.assert_frame_equal(result, expected)
        mock_snapshots.assert_called_once()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
