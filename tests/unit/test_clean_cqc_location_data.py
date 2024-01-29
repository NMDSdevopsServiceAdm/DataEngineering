import unittest
from unittest.mock import ANY, patch

import jobs.clean_cqc_location_data as job

from schemas.cqc_location_schema import LOCATION_SCHEMA
from tests.test_file_data import CQCLocationsData as Data
from tests.test_file_schemas import CQCLocationsSchema

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

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(self, read_from_parquet_patch, write_to_parquet_patch):
        read_from_parquet_patch.return_value = self.test_clean_cqc_location_df
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )

    def test_allocate_primary_service_type(self):
        PRIMARY_SERVICE_TYPE_COLUMN_NAME = "primary_service_type"

        test_primary_service_df = self.spark.createDataFrame(
            Data.primary_service_type_rows,
            schema=CQCLocationsSchema.primary_service_type_schema,
        )

        new_df = job.allocate_primary_service_type(test_primary_service_df)

        self.assertTrue(PRIMARY_SERVICE_TYPE_COLUMN_NAME in new_df.columns)

        rows = new_df.collect()
        self.assertEqual(rows[0][PRIMARY_SERVICE_TYPE_COLUMN_NAME], "non-residential")
        self.assertEqual(
            rows[1][PRIMARY_SERVICE_TYPE_COLUMN_NAME], "Care home with nursing"
        )
        self.assertEqual(
            rows[2][PRIMARY_SERVICE_TYPE_COLUMN_NAME], "Care home without nursing"
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
