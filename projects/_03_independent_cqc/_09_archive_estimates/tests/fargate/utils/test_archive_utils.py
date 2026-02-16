import unittest
from dataclasses import fields
from datetime import datetime
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._09_archive_estimates.fargate.utils.archive_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ArchiveFilledPostsEstimates as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ArchiveFilledPostsEstimates as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveColumns,
    ArchivePartitionKeys,
)

PATCH_PATH: str = (
    "projects._03_independent_cqc._09_archive_estimates.fargate.utils.archive_utils"
)


class SelectImportDatesToArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.input_lf = pl.LazyFrame(
            data=Data.select_import_dates_to_archive_rows,
            schema=Schemas.test_lf_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.add_column_with_the_date_of_most_recent_annual_estimates")
    def test_function_has_expected_calls(
        self, add_column_with_the_date_of_most_recent_annual_estimates_mock: Mock
    ):
        job.select_import_dates_to_archive(
            self.input_lf,
        )
        add_column_with_the_date_of_most_recent_annual_estimates_mock.assert_called_once()

    def test_keeps_earliest_monthly_estimates_from_current_publication_year_and_april_only_from_previous_publication_years(
        self,
    ):
        returned_lf = job.select_import_dates_to_archive(self.input_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_select_import_dates_to_archive_rows,
            schema=Schemas.test_lf_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AddAColumnWithTheDateOfMostRecentAnnualEstimates(unittest.TestCase):
    def setUp(self) -> None:
        self.input_lf = pl.LazyFrame(
            Data.add_column_with_the_date_of_most_recent_annual_estimates_rows,
            Schemas.add_column_with_the_date_of_most_recent_annual_estimates_schema,
            orient="row",
        )
        self.returned_lf = job.add_column_with_the_date_of_most_recent_annual_estimates(
            self.input_lf
        )

    def test_most_recent_annual_estimate_date_column_is_added(
        self,
    ):
        self.assertIn(
            ArchiveColumns.most_recent_annual_estimate_date, self.returned_lf.columns
        )

        cols_added = set(self.returned_lf.columns) - set(self.input_lf.columns)
        self.assertEqual(cols_added, {ArchiveColumns.most_recent_annual_estimate_date})

    def test_most_recent_annual_estimate_date_value_is_april(
        self,
    ):
        expected_lf = pl.LazyFrame(
            Data.expected_add_column_with_the_date_of_most_recent_annual_estimates_rows,
            Schemas.expected_add_column_with_the_date_of_most_recent_annual_estimates_schema,
        )

        pl_testing.assert_frame_equal(self.returned_lf, expected_lf)


class CreateArchiveDatePartitionColumns(unittest.TestCase):
    def setUp(self) -> None:
        self.input_lf = pl.LazyFrame(
            Data.create_archive_date_partition_columns_rows,
            Schemas.create_archive_date_partition_columns_schema,
            orient="row",
        )
        self.expected_when_timestamp_day_and_month_are_single_digits_lf = pl.LazyFrame(
            Data.expected_create_archive_date_partition_columns_when_timestamp_day_and_month_are_single_digits_rows,
            Schemas.expected_create_archive_date_partitions_schema,
        )
        self.expected_when_timestamp_day_and_month_are_double_digits_lf = pl.LazyFrame(
            Data.expected_create_archive_date_partition_columns_when_timestamp_day_and_month_are_double_digits_rows,
            Schemas.expected_create_archive_date_partitions_schema,
        )

    def test_day_month_year_and_timestamp_columns_are_added(
        self,
    ):
        returned_lf = job.create_archive_date_partition_columns(
            self.input_lf, datetime(2025, 1, 1)
        )
        expected_columns_added = [
            field.name for field in fields(ArchivePartitionKeys())
        ]
        for i in range(len(expected_columns_added)):
            self.assertIn(expected_columns_added[i], returned_lf.columns)

        cols_added = set(returned_lf.columns) - set(self.input_lf.columns)
        self.assertEqual(cols_added, set(expected_columns_added))

    def test_expected_day_month_year_and_timestamp_values_are_added_when_timestamp_day_and_month_are_single_digits(
        self,
    ):
        returned_lf = job.create_archive_date_partition_columns(
            self.input_lf, datetime(2025, 1, 1)
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_when_timestamp_day_and_month_are_single_digits_lf
        )

    def test_expected_day_month_year_and_timestamp_values_are_added_when_timestamp_day_and_month_are_double_digits(
        self,
    ):
        returned_lf = job.create_archive_date_partition_columns(
            self.input_lf, datetime(2024, 12, 31)
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_when_timestamp_day_and_month_are_double_digits_lf
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_when_timestamp_day_and_month_are_double_digits_lf
        )
