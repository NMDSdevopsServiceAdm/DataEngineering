import unittest
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F

from utils import utils

import projects._01_ingest.cqc_api.jobs.clean_cqc_provider_data as job

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCProviderData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CQCProviderSchema as Schema,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_values.categorical_column_values import (
    Sector,
)


PATCH_PATH = "projects._01_ingest.cqc_api.jobs.clean_cqc_provider_data"


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_providers_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=Schema.full_schema
        )


class MainTests(CleanCQCProviderDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.add_cqc_sector_column_to_cqc_provider_dataframe")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_mock: Mock,
        column_to_date_mock: Mock,
        add_cqc_sector_column_to_cqc_provider_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_cqc_providers_parquet

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        read_from_parquet_mock.assert_called_once()
        column_to_date_mock.assert_called_once()
        add_cqc_sector_column_to_cqc_provider_dataframe_mock.assert_called_once()
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class CreateLaCqcProviderDataframeTests(CleanCQCProviderDatasetTests):
    def setUp(self) -> None:
        super().setUp()

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
                Sector.local_authority,
                Sector.local_authority,
                Sector.local_authority,
                Sector.local_authority,
            ],
        )


class AddCqcSectorColumnTests(CleanCQCProviderDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        test_cqc_provider_df = self.spark.createDataFrame(
            Data.rows_without_cqc_sector,
            Schema.rows_without_cqc_sector_schema,
        )
        self.test_cqc_provider_with_sector = (
            job.add_cqc_sector_column_to_cqc_provider_dataframe(
                test_cqc_provider_df, Data.sector_rows
            )
        )
        self.test_expected_dataframe = self.spark.createDataFrame(
            Data.expected_rows_with_cqc_sector,
            Schema.expected_rows_with_cqc_sector_schema,
        )

    def test_add_cqc_sector_column_to_cqc_provider_dataframe_adds_cqc_sector_column(
        self,
    ):
        self.assertTrue(
            CQCPClean.cqc_sector in self.test_cqc_provider_with_sector.columns
        )

    def test_add_cqc_sector_column_to_cqc_provider_dataframe_returns_expected_df(
        self,
    ):
        expected_data = self.test_expected_dataframe.sort(CQCP.provider_id).collect()
        returned_data = self.test_cqc_provider_with_sector.sort(
            CQCP.provider_id
        ).collect()

        self.assertEqual(
            returned_data,
            expected_data,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
