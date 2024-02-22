from datetime import date
import unittest
from unittest.mock import patch, ANY, Mock

import jobs.clean_ascwds_workplace_data as job

from tests.test_file_data import ASCWDSWorkplaceData as Data
from tests.test_file_schemas import ASCWDSWorkplaceSchemas as Schemas
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.utils import (
    get_spark,
    format_date_fields,
)
import utils.cleaning_utils as cUtils


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_SOURCE = "s3://some_bucket/some_source_key"
    TEST_DESTINATION = "s3://some_bucket/some_destination_key"
    partition_keys = [
        PartitionKeys.year,
        PartitionKeys.month,
        PartitionKeys.day,
        PartitionKeys.import_date,
    ]

    def setUp(self) -> None:
        self.spark = get_spark()
        self.test_ascwds_worker_df = self.spark.createDataFrame(
            Data.workplace_rows, Schemas.workplace_schema
        )

        self.filled_posts_columns = [AWP.total_staff, AWP.worker_records]


class MainTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.format_date_fields", wraps=format_date_fields)
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
        format_date_fields_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_ascwds_worker_df

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        self.assertEqual(format_date_fields_mock.call_count, 1)

        read_from_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class CastToIntTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_cast_to_int_returns_strings_formatted_as_ints_to_ints(self):
        cast_to_int_df = self.spark.createDataFrame(
            Data.cast_to_int_rows, Schemas.cast_to_int_schema
        )
        cast_to_int_expected_df = self.spark.createDataFrame(
            Data.cast_to_int_expected_rows, Schemas.cast_to_int_expected_schema
        )

        returned_df = job.cast_to_int(cast_to_int_df, self.filled_posts_columns)

        returned_data = returned_df.sort(AWP.location_id).collect()
        expected_data = cast_to_int_expected_df.sort(AWP.location_id).collect()

        self.assertEqual(expected_data, returned_data)

    def test_cast_to_int_returns_strings_not_formatted_as_ints_as_none(self):
        cast_to_int_with_errors_df = self.spark.createDataFrame(
            Data.cast_to_int_errors_rows, Schemas.cast_to_int_schema
        )
        cast_to_int_with_errors_expected_df = self.spark.createDataFrame(
            Data.cast_to_int_errors_expected_rows, Schemas.cast_to_int_expected_schema
        )

        returned_df = job.cast_to_int(
            cast_to_int_with_errors_df, self.filled_posts_columns
        )

        returned_data = returned_df.sort(AWP.location_id).collect()
        expected_data = cast_to_int_with_errors_expected_df.sort(
            AWP.location_id
        ).collect()

        self.assertEqual(expected_data, returned_data)


class AddPurgeOutdatedWorkplacesColumnTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self):
        super().setUp()
        self.test_purge_outdated_df = self.spark.createDataFrame(
            Data.purge_outdated_data, Schemas.purge_outdated_schema
        )
        self.test_purge_outdated_df = cUtils.column_to_date(
            self.test_purge_outdated_df, "import_date", "ascwds_workplace_import_date"
        )
        self.test_purge_outdated_df = self.test_purge_outdated_df.select(
            "locationid",
            "orgid",
            "mupddate",
            "isparent",
            "ascwds_workplace_import_date",
        )

    def test_returned_df_has_new_purge_data_column(self):
        returned_df = job.add_purge_outdated_workplaces_column(
            self.test_purge_outdated_df, "ascwds_workplace_import_date"
        )
        original_cols = self.test_purge_outdated_df.columns
        expected_cols = original_cols + ["purge_data"]
        self.assertCountEqual(returned_df.columns, expected_cols)

    def test_returned_df_purge_data_col_is_string(self):
        returned_df = job.add_purge_outdated_workplaces_column(
            self.test_purge_outdated_df, "ascwds_workplace_import_date"
        )
        self.assertEqual(
            returned_df.select("purge_data").dtypes, [("purge_data", "string")]
        )

    def test_adds_correct_value_for_non_parent_workplaces(self):
        input_df = self.test_purge_outdated_df.where("isparent == 0")

        returned_df = job.add_purge_outdated_workplaces_column(
            input_df, "ascwds_workplace_import_date"
        )

        purge_data_list = [
            row.purge_data for row in returned_df.sort(AWP.location_id).collect()
        ]
        expected_purge_list = ["keep", "purge", "keep", "purge"]
        self.assertEqual(purge_data_list, expected_purge_list)

    def test_adds_correct_value_for_parent_workplaces(self):
        returned_df = job.add_purge_outdated_workplaces_column(
            self.test_purge_outdated_df, "ascwds_workplace_import_date"
        )

        returned_df_parents = returned_df.where("isparent == 1")
        purge_data_list = [
            row.purge_data
            for row in returned_df_parents.sort(AWP.location_id).collect()
        ]
        expected_purge_list = ["keep", "purge"]

        self.assertEqual(purge_data_list, expected_purge_list)

    def test_does_not_use_org_children_with_different_import_dates(self):
        self.org_child = self.spark.createDataFrame(
            [
                (
                    "1-000000007",
                    "20210101",
                    "2",
                    date(2021, 1, 1),
                    0,
                ),
            ],
            Schemas.purge_outdated_schema,
        )
        self.input_df = self.test_purge_outdated_df.union(self.org_child)

        returned_df = job.add_purge_outdated_workplaces_column(
            self.input_df, "ascwds_workplace_import_date"
        )

        returned_df_parents = returned_df.where("isparent == 1")
        purge_data_list = [
            row.purge_data
            for row in returned_df_parents.sort(AWP.location_id).collect()
        ]
        expected_purge_list = ["keep", "purge"]

        self.assertEqual(purge_data_list, expected_purge_list)


class AddColumnWithRepeatedValuesRemovedTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self):
        super().setUp()
        self.test_purge_outdated_df = self.spark.createDataFrame(
            Data.repeated_value_rows, Schemas.repeated_value_schema
        )
        self.df_with_rank__df = self.spark.createDataFrame(
            Data.ranked_rows, Schemas.rank_schema
        )
        self.df_with_lagged_value__df = self.spark.createDataFrame(
            Data.lagged_value_rows, Schemas.lagged_value_schema
        )
        self.expected_df_without_repeated_values_df = self.spark.createDataFrame(
            Data.expected_without_repeated_values_rows,
            Schemas.expected_without_repeated_values_schema,
        )

    def test_one(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
