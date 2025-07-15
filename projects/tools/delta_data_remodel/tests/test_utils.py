import unittest
from unittest.mock import patch, Mock

from polars import DataFrame, concat, col
from polars.testing import assert_frame_equal

from projects.tools.delta_data_remodel.jobs.utils import (
    list_bucket_objects,
    snapshots,
    build_full_table_from_delta,
    build_snapshot_table_from_delta,
    get_diffs,
)

PATCH_PATH = "projects._01_ingest.ascwds.jobs.ingest_ascwds_dataset"


class UtilsTests(unittest.TestCase):
    def setUp(self):
        self.base_snapshot = DataFrame(
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

        self.first_delta = DataFrame(
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

        self.second_full_snapshot = DataFrame(
            {
                "import_date": [
                    20130401,
                    20130401,
                    20130401,
                    20130401,
                    20130401,
                ],
                "providerId": ["a", "b", "c", "d", "f"],
                "value": ["same", "different", "same", "different", "new"],
                "deregistrationDate": ["", "", "", "", ""],
            }
        )

    @patch("boto3.client")
    def test_list_bucket_objects(self, mock_client: Mock):
        # Given
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "root/folder/sub_folder/file.pq"},
                {"Key": "root/extra_folder/folder=folder/sub_folder/file.pq"},
                {"Key": "root/folder=folder/file.pq"},
                {"Key": "root/folder1/34545343/file.pq"},
                {"Key": "root/folder/sub_folder/file.pq"},
                {"Key": "root=root/folder=folder/sub_folder=sub_folder/file.pq"},
            ]
        }

        expected = {
            "root",
            "root=root/folder=folder",
            "root/extra_folder/folder=folder",
            "root/folder",
            "root/folder1",
        }

        # When
        response = list_bucket_objects("bucket", "prefix")

        # Then
        assert set(response) == expected
        mock_client.assert_called_once_with("s3")
        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="bucket", Prefix="prefix"
        )

    @patch("projects.tools.delta_data_remodel.jobs.utils.read_parquet")
    def test_snapshots(self, mock_read_parquet: Mock):
        # Given
        mock_read_parquet.return_value = self.base_snapshot

        expected = self.base_snapshot

        # When
        generator = snapshots("bucket", "read_folder")

        # Then
        assert_frame_equal(next(generator), expected)
        with self.assertRaises(StopIteration):
            next(generator)

        mock_read_parquet.assert_called_once()

    @patch("projects.tools.delta_data_remodel.jobs.utils.read_parquet")
    def test_snapshots_multiple_timepoints(self, mock_read_parquet: Mock):
        # Given
        mock_read_parquet.return_value = concat([self.base_snapshot, self.first_delta])

        expected_first = self.base_snapshot
        expected_second = self.second_full_snapshot

        # When
        generator = snapshots("bucket", "read_folder")

        # Then
        assert_frame_equal(next(generator), expected_first, check_row_order=False)
        assert_frame_equal(
            next(generator).drop(["year", "month", "day"]),
            expected_second,
            check_row_order=False,
        )
        with self.assertRaises(StopIteration):
            next(generator)

        mock_read_parquet.assert_called_once()

    @patch("projects.tools.delta_data_remodel.jobs.utils.snapshots")
    def test_build_full_table_from_delta(self, mock_snapshots: Mock):
        # Given
        mock_snapshots.return_value = [
            self.base_snapshot,
        ]

        expected = self.base_snapshot

        # When
        result = build_full_table_from_delta("bucket", "read_folder")

        # Then
        assert_frame_equal(result, expected)
        mock_snapshots.assert_called_once()

    @patch("projects.tools.delta_data_remodel.jobs.utils.snapshots")
    def test_build_full_table_from_delta_multiple_dates(self, mock_snapshots: Mock):
        # Given
        mock_snapshots.return_value = [
            self.base_snapshot,
            self.second_full_snapshot,
        ]

        expected = concat([self.base_snapshot, self.second_full_snapshot])

        # When
        result = build_full_table_from_delta("bucket", "read_folder").filter(
            col("deregistrationDate").eq("")
        )

        # Then
        assert_frame_equal(result, expected)
        mock_snapshots.assert_called_once()

    @patch("projects.tools.delta_data_remodel.jobs.utils.snapshots")
    def test_build_snapshot_table_from_delta(self, mock_snapshots: Mock):
        # Given
        mock_snapshots.return_value = [
            self.base_snapshot,
        ]

        expected = self.base_snapshot

        # When
        result = build_snapshot_table_from_delta(
            "bucket", "read_folder", timepoint=20130301
        )

        # Then
        assert_frame_equal(result, expected)
        mock_snapshots.assert_called_once()

    def test_get_diffs(self):
        # When
        expected = self.first_delta

        result = get_diffs(
            self.base_snapshot,
            self.second_full_snapshot,
            snapshot_date="20130401",
            primary_key="providerId",
            change_cols=["value"],
        )

        # Then
        assert_frame_equal(result, expected)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
