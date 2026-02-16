import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._09_archive_estimates.fargate.archive_filled_posts_estimates as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)

PATCH_PATH = "projects._03_independent_cqc._09_archive_estimates.fargate.archive_filled_posts_estimates"


class IndCQCArchiveTests(unittest.TestCase):
    TEST_ESTIMATES_SOURCE = "some/directory"
    TEST_DESTINATION = "an/other/directory"
    partition_keys = [
        ArchiveKeys.archive_year,
        ArchiveKeys.archive_month,
        ArchiveKeys.archive_day,
        ArchiveKeys.archive_timestamp,
    ]

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.aUtils.create_archive_date_partition_columns")
    @patch(f"{PATCH_PATH}.aUtils.select_import_dates_to_archive")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        select_import_dates_to_archive_mock: Mock,
        create_archive_date_partition_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_ESTIMATES_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        select_import_dates_to_archive_mock.assert_called_once()
        create_archive_date_partition_columns_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            partition_cols=self.partition_keys,
            append=True,
        )
