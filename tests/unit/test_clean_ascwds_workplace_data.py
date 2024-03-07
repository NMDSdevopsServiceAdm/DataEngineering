from datetime import date
import datetime
import unittest
from unittest.mock import patch, ANY, Mock

from pyspark.sql.dataframe import DataFrame

import jobs.clean_ascwds_workplace_data as job

from tests.test_file_data import ASCWDSWorkplaceData as Data
from tests.test_file_schemas import ASCWDSWorkplaceSchemas as Schemas
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
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
        self.test_ascwds_workplace_df = self.spark.createDataFrame(
            Data.workplace_rows, Schemas.workplace_schema
        )

        self.filled_posts_columns = [AWP.total_staff, AWP.worker_records]


class MainTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(
        "jobs.clean_ascwds_workplace_data.create_column_with_repeated_values_removed"
    )
    @patch("utils.cleaning_utils.set_column_bounds")
    @patch("utils.utils.format_date_fields", wraps=format_date_fields)
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
        format_date_fields_mock: Mock,
        set_column_bounds_mock: Mock,
        create_column_with_repeated_values_removed_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_ascwds_workplace_df

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        self.assertEqual(format_date_fields_mock.call_count, 1)
        self.assertEqual(create_column_with_repeated_values_removed_mock.call_count, 2)
        self.assertEqual(set_column_bounds_mock.call_count, 2)

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
        org_child = self.spark.createDataFrame(
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
        input_df = self.test_purge_outdated_df.union(org_child)

        returned_df = job.add_purge_outdated_workplaces_column(
            input_df, "ascwds_workplace_import_date"
        )

        returned_df_parents = returned_df.where("isparent == 1")
        purge_data_list = [
            row.purge_data
            for row in returned_df_parents.sort(AWP.location_id).collect()
        ]
        expected_purge_list = ["keep", "purge"]

        self.assertEqual(purge_data_list, expected_purge_list)


class PurgeOutdatedWorkplacesColumn(AddPurgeOutdatedWorkplacesColumnTests):
    def setUp(self):
        super().setUp()
        self.test_only_keep_df = job.add_purge_outdated_workplaces_column(
            self.test_purge_outdated_df, "ascwds_workplace_import_date"
        )

    def test_data_correctly_purged(self):
        returned_df = job.purge_outdated_workplaces(self.test_only_keep_df)

        purge_data_list = [
            row.purge_data for row in returned_df.sort(AWP.location_id).collect()
        ]
        expected_list = ["keep", "keep", "keep"]
        self.assertFalse("purge" in purge_data_list)
        self.assertEqual(purge_data_list, expected_list)


class AddColumnWithRepeatedValuesRemovedTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self):
        super().setUp()
        self.test_purge_outdated_df = self.spark.createDataFrame(
            Data.repeated_value_rows, Schemas.repeated_value_schema
        )
        self.expected_df_without_repeated_values_df = self.spark.createDataFrame(
            Data.expected_without_repeated_values_rows,
            Schemas.expected_without_repeated_values_schema,
        )
        self.returned_df = job.create_column_with_repeated_values_removed(
            self.test_purge_outdated_df,
            column_to_clean="integer_column",
        )
        self.OUTPUT_COLUMN = "integer_column_deduplicated"

        self.returned_data = self.returned_df.sort(
            AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date
        ).collect()
        self.expected_data = self.expected_df_without_repeated_values_df.sort(
            AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date
        ).collect()

    def test_first_submitted_value_is_included_in_new_column(self):
        self.assertEqual(
            self.returned_data[0][self.OUTPUT_COLUMN],
            self.expected_data[0][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data[4][self.OUTPUT_COLUMN],
            self.expected_data[4][self.OUTPUT_COLUMN],
        )

    def test_submitted_value_is_included_if_it_wasnt_repeated(self):
        self.assertEqual(
            self.returned_data[1][self.OUTPUT_COLUMN],
            self.expected_data[1][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data[5][self.OUTPUT_COLUMN],
            self.expected_data[5][self.OUTPUT_COLUMN],
        )

    def test_repeated_value_entered_as_null_value(self):
        self.assertEqual(
            self.returned_data[2][self.OUTPUT_COLUMN],
            self.expected_data[2][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data[7][self.OUTPUT_COLUMN],
            self.expected_data[7][self.OUTPUT_COLUMN],
        )

    def test_value_which_has_appeared_before_but_isnt_a_repeat_is_included(self):
        self.assertEqual(
            self.returned_data[6][self.OUTPUT_COLUMN],
            self.expected_data[6][self.OUTPUT_COLUMN],
        )

    def test_returned_df_matches_expected_df(self):
        self.assertEqual(
            self.returned_data,
            self.expected_data,
        )

    def test_returned_df_has_one_additional_column(self):
        self.assertEqual(
            len(self.returned_df.columns), len(self.test_purge_outdated_df.columns) + 1
        )

    def test_returned_df_has_same_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_purge_outdated_df.count())


class RemoveLocationsWithDuplicatesTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_location_df = self.spark.createDataFrame(
            Data.small_location_rows, Schemas.location_schema
        )

        self.returned_df = job.remove_locations_with_duplicates(self.test_location_df)

        self.test_duplicate_loc_df = self.spark.createDataFrame(
            Data.location_rows_with_duplicates, Schemas.location_schema
        )

    def test_returns_a_dataframe(self):
        self.assertEqual(type(self.returned_df), DataFrame)

    def test_returns_the_correct_columns(self):
        self.assertCountEqual(
            self.returned_df.columns,
            [
                "locationid",
                "import_date",
                "orgid",
            ],
        )

    def test_does_not_remove_rows_if_no_duplicates(self):
        self.assertEqual(self.returned_df.collect(), self.test_location_df.collect())

    def test_removes_duplicate_location_id_with_same_import_date(self):
        filtered_df = job.remove_locations_with_duplicates(self.test_duplicate_loc_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_location_rows, Schemas.location_schema
        )
        self.assertEqual(filtered_df.collect(), expected_df.collect())

    def test_does_not_remove_duplicate_location_id_with_different_import_dates(self):
        locations_with_different_import_dates_df = self.spark.createDataFrame(
            Data.location_rows_with_different_import_dates,
            Schemas.location_schema,
        ).orderBy(AWP.location_id)

        filtered_df = job.remove_locations_with_duplicates(
            locations_with_different_import_dates_df
        ).orderBy(AWP.location_id)

        self.assertEqual(
            filtered_df.collect(), locations_with_different_import_dates_df.collect()
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
