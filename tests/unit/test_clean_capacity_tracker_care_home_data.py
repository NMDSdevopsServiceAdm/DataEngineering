import unittest
from unittest.mock import ANY, Mock, patch

from pyspark.sql.dataframe import DataFrame

import jobs.clean_capacity_tracker_care_home_data as job
from tests.test_file_data import CapacityTrackerCareHomeData as Data
from tests.test_file_schemas import CapacityTrackerCareHomeSchema as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)


class CapacityTrackerCareHomeTests(unittest.TestCase):
    TEST_CAPACITY_TRACKER_SOURCE = "some/dir/source"
    TEST_CAPACITY_TRACKER_DESTINATION = "some/dir/destination"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self) -> None:
        self.spark = utils.get_spark()


class MainTests(CapacityTrackerCareHomeTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.capacity_tracker_care_home_rows,
            Schemas.capacity_tracker_care_home_schema,
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.TEST_CAPACITY_TRACKER_SOURCE, self.TEST_CAPACITY_TRACKER_DESTINATION
        )

        read_from_parquet_mock.assert_any_call(
            self.TEST_CAPACITY_TRACKER_SOURCE, job.CAPACITY_TRACKER_CARE_HOME_COLUMNS
        )
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_CAPACITY_TRACKER_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class RemoveRowsWhereAgencyAndNonAgenyValuesMatchTests(CapacityTrackerCareHomeTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.remove_matching_agency_and_non_agency_rows,
            Schemas.remove_matching_agency_and_non_agency_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_remove_matching_agency_and_non_agency_rows,
            Schemas.remove_matching_agency_and_non_agency_schema,
        )

    def test_remove_rows_where_agency_and_non_agency_values_match_returns_correct_data(
        self,
    ):
        returned_df = job.remove_rows_where_agency_and_non_agency_values_match(
            self.test_df
        )
        self.assertEqual(
            returned_df.sort(CTCH.cqc_id).collect(), self.expected_df.collect()
        )


class CreateNewColumnsWithTotalsTests(CapacityTrackerCareHomeTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.create_new_columns_with_totals_rows,
            Schemas.create_new_columns_with_totals_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_new_columns_with_totals_rows,
            Schemas.expected_create_new_columns_with_totals_schema,
        )

    def test_create_new_columns_with_totals_returns_correct_data(
        self,
    ):
        returned_df = job.create_new_columns_with_totals(self.test_df)
        self.assertEqual(
            returned_df.sort(CTCH.cqc_id).collect(), self.expected_df.collect()
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
