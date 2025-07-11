import unittest
from unittest.mock import patch, Mock
from pyspark.sql import DataFrame

import projects._01_ingest.ascwds.jobs.clean_ascwds_workplace_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ASCWDSWorkplaceData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ASCWDSWorkplaceSchemas as Schemas,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils import utils

PATCH_PATH = "projects._01_ingest.ascwds.jobs.clean_ascwds_workplace_data"


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

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.select_columns_required_for_reconciliation_df")
    @patch(f"{PATCH_PATH}.cUtils.set_column_bounds")
    @patch(f"{PATCH_PATH}.cUtils.cast_to_int")
    @patch(f"{PATCH_PATH}.cUtils.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date")
    @patch(f"{PATCH_PATH}.utils.format_date_fields", wraps=utils.format_date_fields)
    @patch(f"{PATCH_PATH}.remove_duplicate_workplaces_in_raw_workplace_data")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        remove_duplicate_workplaces_in_raw_workplace_data: Mock,
        format_date_fields_mock: Mock,
        column_to_date_mock: Mock,
        apply_categorical_labels_mock: Mock,
        cast_to_int_mock: Mock,
        set_column_bounds_mock: Mock,
        select_columns_required_for_reconciliation_df_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_ascwds_workplace_df

        job.main(
            self.TEST_SOURCE,
            self.TEST_CLEANED_DESTINATION,
            self.TEST_RECONCILIATION_DESTINATION,
        )

        read_from_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        remove_duplicate_workplaces_in_raw_workplace_data.assert_called_once()
        format_date_fields_mock.assert_called_once()
        column_to_date_mock.assert_called_once()
        apply_categorical_labels_mock.assert_called_once()
        cast_to_int_mock.assert_called_once()
        self.assertEqual(set_column_bounds_mock.call_count, 2)
        select_columns_required_for_reconciliation_df_mock.assert_called_once()
        self.assertEqual(write_to_parquet_mock.call_count, 2)


class FilterTestAccountsTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self):
        super().setUp()

    def test_filter_test_accounts(self):
        test_df = self.spark.createDataFrame(
            Data.filter_test_account_when_orgid_present_rows,
            Schemas.filter_test_account_when_orgid_present_schema,
        )

        returned_df = job.filter_test_accounts(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_filter_test_account_when_orgid_present_rows,
            Schemas.filter_test_account_when_orgid_present_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_filter_test_accounts_without_orgid_doesnt_filter_rows(self):
        test_df = self.spark.createDataFrame(
            Data.filter_test_account_when_orgid_not_present_rows,
            Schemas.filter_test_account_when_orgid_not_present_schema,
        )

        returned_df = job.filter_test_accounts(test_df)

        self.assertEqual(
            returned_df.sort(AWP.location_id).collect(),
            test_df.sort(AWP.location_id).collect(),
        )


class RemoveWhiteSpaceFromNmdsidTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_remove_white_space_from_nmdsid(self):
        test_df = self.spark.createDataFrame(
            Data.remove_white_space_from_nmdsid_rows,
            Schemas.remove_white_space_from_nmdsid_schema,
        )

        returned_df = job.remove_white_space_from_nmdsid(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_remove_white_space_from_nmdsid_rows,
            Schemas.remove_white_space_from_nmdsid_schema,
        )

        self.assertEqual(
            returned_df.sort(AWP.location_id).collect(),
            expected_df.sort(AWP.location_id).collect(),
        )


class CreatePurgedDfsForReconciliationAndDataTests(CleanASCWDSWorkplaceDatasetTests):
    def setUp(self):
        super().setUp()

    @patch(f"{PATCH_PATH}.create_date_column_for_purging_data")
    @patch(f"{PATCH_PATH}.create_workplace_last_active_date_column")
    @patch(f"{PATCH_PATH}.create_data_last_amended_date_column")
    @patch(f"{PATCH_PATH}.calculate_maximum_master_update_date_for_organisation")
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
