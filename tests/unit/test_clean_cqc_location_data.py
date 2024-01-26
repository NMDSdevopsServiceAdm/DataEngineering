import unittest
from unittest.mock import patch

import jobs.clean_cqc_location_data as job

from schemas.cqc_location_schema import LOCATION_SCHEMA
from tests.test_file_data import CQCLocationsData as Data

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class CleanCQCLocationDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.sample_rows_full, schema=LOCATION_SCHEMA
        )

    def test_clean_cqc_location_df_returns_the_same_dataframe_it_is_passed_in(self):
        returned_df = job.clean_cqc_location_df(self.test_clean_cqc_location_df)

        self.assertEqual(self.test_clean_cqc_location_df, returned_df)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(self, read_from_parquet_patch, write_to_parquet_patch):
        read_from_parquet_patch.return_value = self.test_clean_cqc_location_df
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            self.test_clean_cqc_location_df,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
