import unittest

from unittest.mock import ANY, Mock, patch


import jobs.merge_ind_cqc_data as job


from tests.test_file_data import MergeIndCQCData as Data
from tests.test_file_schemas import MergeIndCQCData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class CleanCQCLocationDatasetTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_CQC_PIR_SOURCE = "some/other/directory"
    TEST_ASCWDS_WORKPLACE_SOURCE = "some/other/directory"
    TEST_ONS_POSTCODE_DIRECTORY_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.clean_cqc_location_rows, Schemas.clean_cqc_location_schema
        )
        self.test_clean_cqc_pir_df = self.spark.createDataFrame(
            Data.clean_cqc_pir_rows, Schemas.clean_cqc_pir_schema
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.clean_ascwds_workplace_rows, Schemas.clean_ascwds_workplace_schema
        )
        self.test_ons_postcode_directory_df = self.spark.createDataFrame(
            Data.ons_postcode_directory_rows, Schemas.ons_postcode_directory_schema
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_clean_cqc_pir_df,
            self.test_clean_ascwds_workplace_df,
            self.test_ons_postcode_directory_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_CQC_PIR_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_ONS_POSTCODE_DIRECTORY_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 4)

        write_to_parquet_patch.assert_called_once_with(
            self.test_clean_cqc_location_df,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_remove_non_social_care_locations_only_keeps_social_care_orgs(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.cqc_sector_rows, Schemas.cqc_sector_schema
        )

        returned_ind_cqc_df = job.filter_df_to_independent_sector_only(test_df)
        returned_ind_cqc_data = returned_ind_cqc_df.collect()

        expected_ind_cqc_data = self.spark.createDataFrame(
            Data.expected_cqc_sector_rows, Schemas.cqc_sector_schema
        ).collect()

        self.assertEqual(returned_ind_cqc_data, expected_ind_cqc_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
