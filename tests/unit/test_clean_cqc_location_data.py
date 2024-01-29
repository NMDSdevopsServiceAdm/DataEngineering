import unittest
from unittest.mock import ANY, patch
import pyspark.sql.functions as F

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

        output_df = job.allocate_primary_service_type(test_primary_service_df)

        self.assertTrue(PRIMARY_SERVICE_TYPE_COLUMN_NAME in output_df.columns)

        primary_service_values = output_df.select(
            F.collect_list(PRIMARY_SERVICE_TYPE_COLUMN_NAME)
        ).first()[0]

        self.assertEqual(len(primary_service_values), 5)
        self.assertEqual(primary_service_values[0], job.NONE_RESIDENTIAL_IDENTIFIER)
        self.assertEqual(primary_service_values[1], job.NURSING_HOME_IDENTIFIER)
        self.assertEqual(primary_service_values[2], job.NONE_NURSING_HOME_IDENTIFIER)
        self.assertEqual(primary_service_values[3], job.NURSING_HOME_IDENTIFIER)
        self.assertEqual(primary_service_values[4], job.NONE_NURSING_HOME_IDENTIFIER)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
