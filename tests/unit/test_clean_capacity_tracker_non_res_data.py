import unittest
from unittest.mock import ANY, Mock, patch

import jobs.clean_capacity_tracker_non_res_data as job
from tests.test_file_data import CapacityTrackerNonResData as Data
from tests.test_file_schemas import CapacityTrackerNonResSchema as Schemas
from utils import utils
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


class CapacityTrackerNonResTests(unittest.TestCase):
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


class MainTests(CapacityTrackerNonResTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.capacity_tracker_non_res_rows,
            Schemas.capacity_tracker_non_res_schema,
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.TEST_CAPACITY_TRACKER_SOURCE, self.TEST_CAPACITY_TRACKER_DESTINATION
        )

        read_from_parquet_mock.assert_any_call(
            self.TEST_CAPACITY_TRACKER_SOURCE, job.CAPACITY_TRACKER_NON_RES_COLUMNS
        )
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_CAPACITY_TRACKER_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class CalculateCapacityTrackerRollingAverageTests(CapacityTrackerNonResTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_capacity_tracker_rolling_average_calculates_a_rolling_average(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.capacity_tracker_non_res_rolling_average_rows,
            Schemas.capacity_tracker_non_res_rolling_average_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_capacity_tracker_non_res_rolling_average_rows,
            Schemas.expected_capacity_tracker_non_res_rolling_average_schema,
        )
        returned_df = job.calculate_capacity_tracker_rolling_average(test_df)
        self.assertEqual(
            returned_df.sort(
                CTNR.cqc_id, CTNRClean.capacity_tracker_import_date
            ).collect(),
            expected_df.collect(),
        )

    def test_calculate_capacity_tracker_rolling_average_calculates_rolling_average_correctly_when_date_spans_over_185_days(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.capacity_tracker_non_res_rolling_average_over_six_months_rows,
            Schemas.capacity_tracker_non_res_rolling_average_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_capacity_tracker_non_res_rolling_average_over_six_months_rows,
            Schemas.expected_capacity_tracker_non_res_rolling_average_schema,
        )
        returned_df = job.calculate_capacity_tracker_rolling_average(test_df)
        self.assertEqual(
            returned_df.sort(
                CTNR.cqc_id, CTNRClean.capacity_tracker_import_date
            ).collect(),
            expected_df.collect(),
        )

    def test_calculate_capacity_tracker_rolling_average_calculates_rolling_average_separately_for_each_location(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.capacity_tracker_non_res_rolling_average_by_location_rows,
            Schemas.capacity_tracker_non_res_rolling_average_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_capacity_tracker_non_res_rolling_average_by_location_rows,
            Schemas.expected_capacity_tracker_non_res_rolling_average_schema,
        )
        returned_df = job.calculate_capacity_tracker_rolling_average(test_df)
        self.assertEqual(
            returned_df.sort(
                CTNR.cqc_id, CTNRClean.capacity_tracker_import_date
            ).collect(),
            expected_df.collect(),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
