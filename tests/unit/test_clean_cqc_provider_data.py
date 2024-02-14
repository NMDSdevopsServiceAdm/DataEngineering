import unittest
from unittest.mock import ANY, patch
import pyspark.sql.functions as F

from utils import utils

import jobs.clean_cqc_provider_data as job

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from tests.test_file_data import CQCProviderData as Data
from tests.test_file_schemas import CQCProviderSchema as Schema
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
    CqcProviderCleanedValues as CQCPCleanValues,
)


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_providers_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=Schema.full_schema
        )

    def test_create_dataframe_from_la_cqc_provider_list_creates_a_dataframe_with_a_column_of_providerids_and_a_column_of_strings(
        self,
    ):
        test_la_cqc_dataframe = job.create_dataframe_from_la_cqc_provider_list(
            Data.sector_rows
        )
        self.assertEqual(
            test_la_cqc_dataframe.columns, [CQCP.provider_id, CQCPClean.cqc_sector]
        )

        test_cqc_sector_list = test_la_cqc_dataframe.select(
            F.collect_list(CQCPClean.cqc_sector)
        ).first()[0]
        self.assertEqual(
            test_cqc_sector_list,
            [
                CQCPCleanValues.local_authority,
                CQCPCleanValues.local_authority,
                CQCPCleanValues.local_authority,
                CQCPCleanValues.local_authority,
            ],
        )

    def test_add_cqc_sector_column_to_cqc_provider_dataframe(self):
        test_cqc_provider_with_sector = (
            job.add_cqc_sector_column_to_cqc_provider_dataframe(
                self.test_cqc_providers_parquet, Data.sector_rows
            )
        )
        self.assertTrue(CQCPClean.cqc_sector in test_cqc_provider_with_sector.columns)

        test_expected_dataframe = self.spark.createDataFrame(
            Data.expected_rows_with_cqc_sector,
            Schema.expected_rows_with_cqc_sector_schema,
        )
        expected_data = test_expected_dataframe.sort(CQCP.provider_id).collect()

        returned_data = (
            test_cqc_provider_with_sector.select(CQCP.provider_id, CQCPClean.cqc_sector)
            .sort(CQCP.provider_id)
            .collect()
        )

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
            ANY,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
