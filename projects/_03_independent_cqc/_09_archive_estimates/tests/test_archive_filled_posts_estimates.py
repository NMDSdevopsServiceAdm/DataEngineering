import unittest
import warnings
from datetime import datetime
from unittest.mock import ANY, Mock, patch

from pyspark.sql import functions as F


import projects._03_independent_cqc._09_archive_estimates.jobs.archive_filled_posts_estimates as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ArchiveFilledPostsEstimates as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ArchiveFilledPostsEstimates as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
    IndCqcColumns as IndCQC,
)

PATCH_PATH: str = "projects._03_independent_cqc._09_archive_estimates.jobs.archive_filled_posts_estimates"


class ArchiveFilledPostsEstimatesTests(unittest.TestCase):
    FILLED_POSTS_ESTIMATES_SOURCE = "some/data"
    MONTHLY_ARCHIVE_DESTINATION = "some/destination"
    ANNUAL_ARCHIVE_DESTINATION = "another/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(ArchiveFilledPostsEstimatesTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_filled_posts_estimates_df = self.spark.createDataFrame(
            Data.filled_posts_rows, Schemas.filled_posts_schema
        )
        self.partition_keys = [
            ArchiveKeys.archive_year,
            ArchiveKeys.archive_month,
            ArchiveKeys.archive_day,
            ArchiveKeys.archive_timestamp,
        ]

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_filled_posts_estimates_df

        job.main(
            self.FILLED_POSTS_ESTIMATES_SOURCE,
            self.MONTHLY_ARCHIVE_DESTINATION,
            self.ANNUAL_ARCHIVE_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)

        self.assertEqual(write_to_parquet_patch.call_count, 1)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.MONTHLY_ARCHIVE_DESTINATION,
            mode="append",
            partitionKeys=self.partition_keys,
        )


class SelectImportDatesToArchiveTests(ArchiveFilledPostsEstimatesTests):
    def setUp(self) -> None:
        super().setUp()
        test_df = self.spark.createDataFrame(
            Data.select_import_dates_to_archive_rows,
            Schemas.select_import_dates_to_archive_schema,
        )
        self.test_data = test_df.collect()
        self.expected_data = self.spark.createDataFrame(
            Data.expected_select_import_dates_to_archive_rows,
            Schemas.select_import_dates_to_archive_schema,
        ).collect()
        self.returned_data = (
            job.select_import_dates_to_archive(test_df)
            .sort(F.desc(IndCQC.cqc_location_import_date))
            .collect()
        )

    def test_select_import_dates_to_archive_keeps_earliest_monthly_estimates_from_current_year(
        self,
    ):
        self.assertEqual(
            self.expected_data[0][IndCQC.cqc_location_import_date],
            self.returned_data[0][IndCQC.cqc_location_import_date],
        )
        self.assertEqual(
            self.expected_data[1][IndCQC.cqc_location_import_date],
            self.returned_data[1][IndCQC.cqc_location_import_date],
        )

    def test_select_import_dates_to_archive_keeps_annual_estimates_for_all_years(self):
        self.assertEqual(
            self.expected_data[2][IndCQC.cqc_location_import_date],
            self.returned_data[2][IndCQC.cqc_location_import_date],
        )
        self.assertEqual(
            self.expected_data[3][IndCQC.cqc_location_import_date],
            self.returned_data[3][IndCQC.cqc_location_import_date],
        )

    def test_select_import_dates_to_archive_removes_all_monthly_estimates_before_current_annual_estimates_years(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertNotEqual(
                self.returned_data[i][IndCQC.cqc_location_import_date],
                self.test_data[3][IndCQC.cqc_location_import_date],
            )
        for i in range(len(self.returned_data)):
            self.assertNotEqual(
                self.returned_data[i][IndCQC.cqc_location_import_date],
                self.test_data[5][IndCQC.cqc_location_import_date],
            )


class CreateArchiveDatePartitionColumnsTests(ArchiveFilledPostsEstimatesTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_date_time = datetime(2024, 1, 2, 12, 0)

    def test_create_archive_date_partition_columns_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.create_archive_date_partitions_rows,
            Schemas.create_archive_date_partitions_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_archive_date_partitions_rows,
            Schemas.expected_create_archive_date_partitions_schema,
        )
        returned_df = job.create_archive_date_partition_columns(
            test_df, self.test_date_time
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class AddLeadingZeroTests(ArchiveFilledPostsEstimatesTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_leading_zero_returns_correct_value_when_passed_single_digit_int(self):
        test_number = Data.single_digit_number
        expected_string = Data.expected_single_digit_number_as_string
        returned_value = job.add_leading_zero(test_number)
        self.assertEqual(returned_value, expected_string)
        self.assertIsInstance(returned_value, str)

    def test_add_leading_zero_returns_correct_value_when_passed_double_digit_int(self):
        test_number = Data.double_digit_number
        expected_string = Data.expected_double_digit_number_as_string
        returned_value = job.add_leading_zero(test_number)
        self.assertEqual(returned_value, expected_string)
        self.assertIsInstance(returned_value, str)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
