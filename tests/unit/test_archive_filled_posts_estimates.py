import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.archive_filled_posts_estimates as job
from tests.test_file_data import ArchiveFilledPostsEstimates as Data
from tests.test_file_schemas import ArchiveFilledPostsEstimates as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


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

    # @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        # write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_filled_posts_estimates_df

        job.main(
            self.FILLED_POSTS_ESTIMATES_SOURCE,
            self.MONTHLY_ARCHIVE_DESTINATION,
            self.ANNUAL_ARCHIVE_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        """
        self.assertEqual(write_to_parquet_patch.call_count, 2)
        write_to_parquet_patch.assert_any_call(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )
        """


class CreateArchiveDatePartitionColumnsTests(ArchiveFilledPostsEstimatesTests):
    def setUp(self) -> None:
        super().setUp()

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
            test_df, IndCQC.cqc_location_import_date
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
