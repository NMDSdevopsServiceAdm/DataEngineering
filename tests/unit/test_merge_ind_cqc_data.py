import unittest

from unittest.mock import ANY, Mock, patch

import jobs.merge_ind_cqc_data as job

from tests.test_file_data import MergeIndCQCData as Data
from tests.test_file_schemas import MergeIndCQCData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


class MergeIndCQCDatasetTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_CQC_PIR_SOURCE = "some/other/directory"
    TEST_ASCWDS_WORKPLACE_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.clean_cqc_location_for_merge_rows,
            Schemas.clean_cqc_location_for_merge_schema,
        )
        self.test_clean_cqc_pir_df = self.spark.createDataFrame(
            Data.clean_cqc_pir_rows, Schemas.clean_cqc_pir_schema
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.clean_ascwds_workplace_for_merge_rows,
            Schemas.clean_ascwds_workplace_for_merge_schema,
        )

    @patch("jobs.merge_ind_cqc_data.join_ascwds_data_into_merged_df")
    @patch(
        "jobs.merge_ind_cqc_data.filter_df_to_independent_sector_only",
        wraps=job.filter_df_to_independent_sector_only,
    )
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
        filter_df_to_independent_sector_only: Mock,
        join_ascwds_data_into_merged_df: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_clean_ascwds_workplace_df,
            self.test_clean_cqc_pir_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_CQC_PIR_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 3)

        filter_df_to_independent_sector_only.assert_called_once_with(
            self.test_clean_cqc_location_df
        )
        join_ascwds_data_into_merged_df.assert_called_once()

        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_filter_df_to_independent_sector_only_keeps_independent_locations(
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

    def test_join_ascwds_data_into_merged_df(self):
        returned_df = job.join_ascwds_data_into_merged_df(
            self.test_clean_cqc_location_df,
            self.test_clean_ascwds_workplace_df,
            CQCLClean.cqc_location_import_date,
            AWPClean.ascwds_workplace_import_date,
        )

        expected_merged_df = self.spark.createDataFrame(
            Data.expected_cqc_and_ascwds_merged_rows,
            Schemas.expected_cqc_and_ascwds_merged_schema,
        )

        returned_data = returned_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()
        expected_data = expected_merged_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()

        self.assertEqual(returned_data, expected_data)

    def test_join_pir_data_into_merged_df(self):
        returned_df = job.join_pir_data_into_merged_df(
            self.test_clean_cqc_location_df,
            self.test_clean_cqc_pir_df,
        )

        expected_merged_df = self.spark.createDataFrame(
            Data.expected_merged_cqc_and_pir,
            Schemas.expected_cqc_and_pir_merged_schema,
        )

        returned_data = returned_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()
        expected_data = expected_merged_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()

        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
