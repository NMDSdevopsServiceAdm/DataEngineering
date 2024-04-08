from datetime import date
import unittest
from unittest.mock import patch, ANY, Mock

from pyspark.sql import DataFrame, functions as F

import jobs.clean_ascwds_workplace_data as job

from tests.test_file_data import ASCWDSWorkplaceData as Data
from tests.test_file_schemas import ASCWDSWorkplaceSchemas as Schemas
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
    AscwdsWorkplaceCleanedValues as AWPValues,
)
from utils.utils import (
    get_spark,
    format_date_fields,
)
import utils.cleaning_utils as cUtils


class CleanASCWDSWorkplaceDatasetTests(unittest.TestCase):
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


class MainTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

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
    ):
        read_from_parquet_mock.return_value = self.test_ascwds_workplace_df

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        read_from_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        self.assertEqual(format_date_fields_mock.call_count, 1)
        self.assertEqual(set_column_bounds_mock.call_count, 2)
        self.assertEqual(write_to_parquet_mock.call_count, 2)


class CastToIntTests(CleanASCWDSWorkplaceDatasetTests):
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


class CreatePurgedDfsForCoverageAndDataTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self):
        super().setUp()

        # TODO


class RemoveWorkplacesWithDuplicateLocationIdsTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_location_df = self.spark.createDataFrame(
            Data.small_location_rows, Schemas.location_schema
        )

        self.returned_df = job.remove_workplaces_with_duplicate_location_ids(
            self.test_location_df
        )

        self.test_duplicate_loc_df = self.spark.createDataFrame(
            Data.location_rows_with_duplicates, Schemas.location_schema
        )

    def test_returns_a_dataframe(self):
        self.assertEqual(type(self.returned_df), DataFrame)

    def test_returns_the_correct_columns(self):
        self.assertCountEqual(
            self.returned_df.columns,
            [
                AWP.location_id,
                AWP.import_date,
                AWP.organisation_id,
            ],
        )

    def test_does_not_remove_rows_if_no_duplicates(self):
        expected_df = self.test_location_df
        self.assertEqual(
            self.returned_df.sort(AWP.organisation_id).collect(),
            expected_df.sort(AWP.organisation_id).collect(),
        )

    def test_removes_duplicate_location_id_with_same_import_date(self):
        returned_df = job.remove_workplaces_with_duplicate_location_ids(
            self.test_duplicate_loc_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_location_rows, Schemas.location_schema
        )
        self.assertEqual(
            returned_df.sort(AWP.organisation_id).collect(),
            expected_df.sort(AWP.organisation_id).collect(),
        )

    def test_does_not_remove_duplicate_location_id_with_different_import_dates(self):
        duplicate_location_id_with_different_import_dates_df = (
            self.spark.createDataFrame(
                Data.location_rows_with_different_import_dates,
                Schemas.location_schema,
            )
        )
        returned_df = job.remove_workplaces_with_duplicate_location_ids(
            duplicate_location_id_with_different_import_dates_df
        )
        expected_df = duplicate_location_id_with_different_import_dates_df

        self.assertEqual(
            returned_df.sort(AWP.organisation_id).collect(),
            expected_df.sort(AWP.organisation_id).collect(),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
