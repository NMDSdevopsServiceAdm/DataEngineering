import unittest
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F

import jobs.clean_cqc_location_data as job

from schemas.cqc_location_schema import LOCATION_SCHEMA
from tests.test_file_data import CQCLocationsData as Data
from tests.test_file_schemas import CQCLocationsSchema as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)


class CleanCQCLocationDatasetTests(unittest.TestCase):
    TEST_LOC_SOURCE = "some/directory"
    TEST_PROV_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.sample_rows_full, schema=LOCATION_SCHEMA
        )
        self.test_location_df = self.spark.createDataFrame(
            Data.small_location_rows, Schemas.small_location_schema
        )
        self.test_provider_df = self.spark.createDataFrame(
            Data.join_provider_rows, Schemas.join_provider_schema
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self, read_from_parquet_patch: Mock, write_to_parquet_patch: Mock
    ):
        read_from_parquet_patch.return_value = self.test_clean_cqc_location_df

        job.main(self.TEST_LOC_SOURCE, self.TEST_PROV_SOURCE, self.TEST_DESTINATION)

        self.assertEqual(read_from_parquet_patch.call_count, 2)
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
            schema=Schemas.primary_service_type_schema,
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

    def test_join_cqc_provider_data_adds_two_columns(self):
        returned_df = job.join_cqc_provider_data(
            self.test_location_df, self.test_provider_df
        )
        new_columns = 2
        expected_columns = len(self.test_location_df.columns) + new_columns

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_join_cqc_provider_data_correctly_joins_data(self):
        returned_df = job.join_cqc_provider_data(
            self.test_location_df, self.test_provider_df
        )
        returned_data = (
            returned_df.select(sorted(returned_df.columns))
            .sort(CQCL.location_id)
            .collect()
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_joined_rows, Schemas.expected_joined_schema
        )
        expected_data = (
            expected_df.select(sorted(expected_df.columns))
            .sort(CQCL.location_id)
            .collect()
        )

        self.assertCountEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
