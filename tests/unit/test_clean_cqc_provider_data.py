import unittest
from unittest.mock import patch
import pyspark.sql.functions as F

from utils import utils

import jobs.clean_cqc_provider_data as job

from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from tests.test_file_data import CQCProviderData as Data
from tests.test_file_schemas import CQCProviderSchema as Schema


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_providers_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=PROVIDER_SCHEMA
        )

    def test_create_dataframe_from_la_cqc_provider_list_creates_a_dataframe_with_a_column_of_providerids_and_a_column_of_strings(
        self,
    ):
        test_la_cqc_dataframe = job.create_dataframe_from_la_cqc_provider_list(
            [
                "1-1000001",
                "1-100002",
            ]
        )
        self.assertEqual(test_la_cqc_dataframe.columns, ["provider_id", "cqc_sector"])

        test_cqc_sector_list = test_la_cqc_dataframe.select(
            F.collect_list("cqc_sector")
        ).first()[0]
        self.assertEqual(test_cqc_sector_list, ["Local authority", "Local authority"])

    def test_add_cqc_sector_column_to_cqc_provider_dataframe(self):
        test_cqc_provider_with_sector = (
            job.add_cqc_sector_column_to_cqc_provider_dataframe(
                self.test_cqc_providers_parquet, Data.sector_rows
            )
        )
        self.assertTrue("cqc_sector" in test_cqc_provider_with_sector.columns)

        test_expected_dataframe = self.spark.createDataFrame(
            Data.expected_schema_with_cqc_sector_columns,
            Schema.expected_schema_with_cqc_sector_schema,
        )

        returned_data = (
            test_cqc_provider_with_sector.select("providerId", "cqc_sector")
            .sort("providerId")
            .collect()
        )
        expected_data = test_expected_dataframe.sort("providerId").collect()

        self.assertEqual(
            returned_data,
            expected_data,
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch, write_to_parquet_patch):
        read_from_parquet_patch.return_value = self.test_cqc_providers_parquet
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            self.test_cqc_providers_parquet,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
