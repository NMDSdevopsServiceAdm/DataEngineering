import unittest
from unittest.mock import patch, Mock
from pyspark.sql import DataFrame

import jobs.clean_ascwds_workplace_data as job

from tests.test_file_data import ASCWDSWorkplaceData as Data
from tests.test_file_schemas import ASCWDSWorkplaceSchemas as Schemas
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils import utils


class CleanASCWDSWorkplaceDatasetTests(unittest.TestCase):
    TEST_SOURCE = "s3://some_bucket/some_source_key"
    TEST_CLEANED_DESTINATION = "s3://some_bucket/some_destination_key"
    TEST_RECONCILIATION_DESTINATION = "s3://some_other_destination_key"
    partition_keys = [
        PartitionKeys.year,
        PartitionKeys.month,
        PartitionKeys.day,
        PartitionKeys.import_date,
    ]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_ascwds_workplace_df = self.spark.createDataFrame(
            Data.workplace_rows, Schemas.workplace_schema
        )

        self.filled_posts_columns = [AWP.total_staff, AWP.worker_records]


class MainTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(
        "jobs.clean_ascwds_workplace_data.select_columns_required_for_reconciliation_df"
    )
    @patch("utils.cleaning_utils.set_column_bounds")
    @patch("utils.cleaning_utils.apply_categorical_labels")
    @patch("utils.utils.format_date_fields", wraps=utils.format_date_fields)
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
        format_date_fields_mock: Mock,
        apply_categorical_labels_mock: Mock,
        set_column_bounds_mock: Mock,
        select_columns_required_for_reconciliation_df_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_ascwds_workplace_df

        job.main(
            self.TEST_SOURCE,
            self.TEST_CLEANED_DESTINATION,
            self.TEST_RECONCILIATION_DESTINATION,
        )

        read_from_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        self.assertEqual(format_date_fields_mock.call_count, 1)
        self.assertEqual(apply_categorical_labels_mock.call_count, 1)
        self.assertEqual(set_column_bounds_mock.call_count, 2)
        self.assertEqual(
            select_columns_required_for_reconciliation_df_mock.call_count, 1
        )
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


class CreatePurgedDfsForReconciliationAndDataTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self):
        super().setUp()

    @patch("jobs.clean_ascwds_workplace_data.create_date_column_for_purging_data")
    @patch("jobs.clean_ascwds_workplace_data.create_workplace_last_active_date_column")
    @patch("jobs.clean_ascwds_workplace_data.create_data_last_amended_date_column")
    @patch(
        "jobs.clean_ascwds_workplace_data.calculate_maximum_master_update_date_for_organisation"
    )
    def test_create_purged_dfs_for_reconciliation_and_data_runs(
        self,
        calculate_maximum_master_update_date_for_organisation_patch: Mock,
        create_data_last_amended_date_column_patch: Mock,
        create_workplace_last_active_date_column_patch: Mock,
        create_date_column_for_purging_data_patch: Mock,
    ):
        job.create_purged_dfs_for_reconciliation_and_data(self.test_ascwds_workplace_df)

        self.assertEqual(
            calculate_maximum_master_update_date_for_organisation_patch.call_count, 1
        )
        self.assertEqual(create_data_last_amended_date_column_patch.call_count, 1)
        self.assertEqual(create_workplace_last_active_date_column_patch.call_count, 1)
        self.assertEqual(create_date_column_for_purging_data_patch.call_count, 1)


class CalculateMaximumMasterUpdateDateForOrganisationTests(
    CleanASCWDSWorkplaceDatasetTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_mupddate_for_org_df = self.spark.createDataFrame(
            Data.mupddate_for_org_rows,
            Schemas.mupddate_for_org_schema,
        )
        self.returned_df = job.calculate_maximum_master_update_date_for_organisation(
            self.test_mupddate_for_org_df,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_mupddate_for_org_rows,
            Schemas.expected_mupddate_for_org_schema,
        )

    def test_calculate_maximum_master_update_date_for_organisation_adds_a_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.test_mupddate_for_org_df.columns) + 1
        self.assertEqual(returned_columns, expected_columns)

    def test_calculate_maximum_master_update_date_for_organisation_returns_expected_df(
        self,
    ):
        self.assertEqual(
            self.returned_df.sort(AWP.location_id).collect(),
            self.expected_df.sort(AWP.location_id).collect(),
        )


class CreateDataPurgeDateColumnTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_add_purge_data_col_df = self.spark.createDataFrame(
            Data.add_purge_data_col_rows,
            Schemas.add_purge_data_col_schema,
        )
        self.returned_df = job.create_data_last_amended_date_column(
            self.test_add_purge_data_col_df,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_purge_data_col_rows,
            Schemas.expected_add_purge_data_col_schema,
        )

    def test_create_data_last_amended_date_column_adds_a_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.test_add_purge_data_col_df.columns) + 1
        self.assertEqual(returned_columns, expected_columns)

    def test_create_data_last_amended_date_column_returns_expected_df(
        self,
    ):
        self.assertEqual(
            self.returned_df.sort(AWP.location_id).collect(),
            self.expected_df.sort(AWP.location_id).collect(),
        )


class CreateReconciliationPurgeDateColumnTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_add_workplace_last_active_date_col_df = self.spark.createDataFrame(
            Data.add_workplace_last_active_date_col_rows,
            Schemas.add_workplace_last_active_date_col_schema,
        )
        self.returned_df = job.create_workplace_last_active_date_column(
            self.test_add_workplace_last_active_date_col_df,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_workplace_last_active_date_col_rows,
            Schemas.expected_add_workplace_last_active_date_col_schema,
        )

    def test_create_workplace_last_active_date_column_adds_a_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = (
            len(self.test_add_workplace_last_active_date_col_df.columns) + 1
        )
        self.assertEqual(returned_columns, expected_columns)

    def test_create_workplace_last_active_date_column_returns_expected_df(
        self,
    ):
        self.assertEqual(
            self.returned_df.sort(AWP.location_id).collect(),
            self.expected_df.sort(AWP.location_id).collect(),
        )


class CreateDateColumnForPurgingDataTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_date_col_for_purging_df = self.spark.createDataFrame(
            Data.date_col_for_purging_rows,
            Schemas.date_col_for_purging_schema,
        )
        self.returned_df = job.create_date_column_for_purging_data(
            self.test_date_col_for_purging_df,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_date_col_for_purging_rows,
            Schemas.expected_date_col_for_purging_schema,
        )

    def test_create_date_column_for_purging_data_adds_a_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.test_date_col_for_purging_df.columns) + 1
        self.assertEqual(returned_columns, expected_columns)

    def test_create_date_column_for_purging_data_returns_expected_df(
        self,
    ):
        self.assertEqual(
            self.returned_df.sort(AWP.location_id).collect(),
            self.expected_df.sort(AWP.location_id).collect(),
        )


class KeepWorkplacesActiveOnOrAfterPurgeDate(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_workplace_last_active_df = self.spark.createDataFrame(
            Data.workplace_last_active_rows,
            Schemas.workplace_last_active_schema,
        )
        self.returned_df = job.keep_workplaces_active_on_or_after_purge_date(
            self.test_workplace_last_active_df, "last_active", AWPClean.purge_date
        )
        self.returned_locations = (
            self.returned_df.select(AWP.establishment_id)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def test_remove_workplace_when_last_active_before_purge_date(self):
        self.assertFalse("1" in self.returned_locations)

    def test_keep_workplace_when_last_active_on_purge_date(self):
        self.assertTrue("2" in self.returned_locations)

    def test_keep_workplace_when_last_active_after_purge_date(self):
        self.assertTrue("3" in self.returned_locations)


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
