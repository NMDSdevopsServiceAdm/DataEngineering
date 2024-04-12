import unittest
import warnings
from datetime import date
from unittest.mock import Mock, patch

import jobs.reconciliation as job
from utils import utils

from tests.test_file_data import ReconciliationData as Data
from tests.test_file_schemas import ReconciliationSchema as Schemas
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)


class ReconciliationTests(unittest.TestCase):
    TEST_CQC_LOCATION_API_SOURCE = "some/source"
    TEST_ASCWDS_WORKPLACE_SOURCE = "another/source"
    TEST_SINGLE_SUB_DESTINATION = "some/destination"
    TEST_PARENT_DESTINATION = "another/destination"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_location_api_df = self.spark.createDataFrame(
            Data.input_cqc_location_api_rows,
            Schemas.input_cqc_location_api_schema,
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.input_ascwds_workplace_rows,
            Schemas.input_ascwds_workplace_schema,
        )

        warnings.simplefilter("ignore", ResourceWarning)


class MainTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("jobs.reconciliation.write_to_csv")
    @patch("utils.utils.read_from_parquet")
    def test_main_run(
        self,
        read_from_parquet_patch: Mock,
        write_to_csv_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_location_api_df,
            self.test_clean_ascwds_workplace_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_API_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_SINGLE_SUB_DESTINATION,
            self.TEST_PARENT_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(write_to_csv_patch.call_count, 2)


class PrepareMostRecentCqcLocationDataTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_prepare_most_recent_cqc_location_df_returns_expected_dataframe(self):
        returned_df = job.prepare_most_recent_cqc_location_df(
            self.test_cqc_location_api_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_prepared_most_recent_cqc_location_rows,
            Schemas.expected_prepared_most_recent_cqc_location_schema,
        )
        returned_data = returned_df.sort(CQCL.location_id).collect()
        expected_data = expected_df.sort(CQCL.location_id).collect()

        self.assertEqual(returned_data, expected_data)


class CollectDatesToUseTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_collect_dates_to_use_return_correct_value(self):
        df = self.spark.createDataFrame(
            Data.dates_to_use_rows, Schemas.dates_to_use_schema
        )
        (
            first_of_most_recent_month,
            first_of_previous_month,
        ) = job.collect_dates_to_use(df)

        self.assertEqual(first_of_most_recent_month, date(2024, 3, 1))
        self.assertEqual(first_of_previous_month, date(2024, 2, 1))


class PrepareLatestCleanedAscwdsWorkforceData(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(
        "jobs.reconciliation.remove_ascwds_head_office_accounts_without_location_ids"
    )
    @patch("jobs.reconciliation.filter_to_cqc_registration_type_only")
    @patch("jobs.reconciliation.get_ascwds_parent_accounts")
    @patch("jobs.reconciliation.add_parents_or_singles_and_subs_col_to_df")
    @patch("jobs.reconciliation.replace_region_id_labels_for_reconciliation")
    @patch("utils.utils.filter_df_to_maximum_value_in_column")
    def test_prepare_latest_cleaned_ascwds_workforce_data_runs(
        self,
        filter_df_to_maximum_value_in_column_patch: Mock,
        replace_region_id_labels_for_reconciliation_patch: Mock,
        add_parents_or_singles_and_subs_col_to_df_patch: Mock,
        get_ascwds_parent_accounts_patch: Mock,
        filter_to_cqc_registration_type_only_patch: Mock,
        remove_ascwds_head_office_accounts_without_location_ids_patch: Mock,
    ):
        job.prepare_latest_cleaned_ascwds_workforce_data(
            self.test_clean_ascwds_workplace_df,
        )

        self.assertEqual(filter_df_to_maximum_value_in_column_patch.call_count, 1)
        self.assertEqual(
            replace_region_id_labels_for_reconciliation_patch.call_count, 1
        )
        self.assertEqual(add_parents_or_singles_and_subs_col_to_df_patch.call_count, 1)
        self.assertEqual(get_ascwds_parent_accounts_patch.call_count, 1)
        self.assertEqual(filter_to_cqc_registration_type_only_patch.call_count, 1)
        self.assertEqual(
            remove_ascwds_head_office_accounts_without_location_ids_patch.call_count, 1
        )


class AddParentsOrSinglesAndSubsColToDf(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

        self.input_df = self.spark.createDataFrame(
            Data.parents_or_singles_and_subs_rows,
            Schemas.parents_or_singles_and_subs_schema,
        )

        self.returned_df = job.add_parents_or_singles_and_subs_col_to_df(self.input_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_parents_or_singles_and_subs_rows,
            Schemas.expected_parents_or_singles_and_subs_schema,
        )

    def test_parents_or_singles_and_subs_column_adds_a_column(self):
        returned_column_count = len(self.returned_df.columns)
        expected_column_count = len(self.input_df.columns) + 1
        self.assertEqual(returned_column_count, expected_column_count)

    def test_parents_or_singles_and_subs_column_adds_correct_labels(self):
        returned_data = self.returned_df.sort(AWPClean.establishment_id).collect()
        expected_data = self.expected_df.sort(AWPClean.establishment_id).collect()

        self.assertEqual(returned_data, expected_data)


class FilterToCqcRegistrationTypeOnly(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_filter_to_cqc_registration_type_df = self.spark.createDataFrame(
            Data.regtype_rows,
            Schemas.regtype_schema,
        )
        self.returned_df = job.filter_to_cqc_registration_type_only(
            self.test_filter_to_cqc_registration_type_df,
        )
        self.returned_locations = (
            self.returned_df.select(AWPClean.establishment_id)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def test_remove_workplace_when_main_service_id_is_not_regulated(self):
        self.assertFalse("1" in self.returned_locations)

    def test_keep_workplace_when_main_service_id_is_cqc_regulated(self):
        self.assertTrue("2" in self.returned_locations)

    def test_remove_workplace_when_main_service_id_is_null(self):
        self.assertFalse("3" in self.returned_locations)


class RemoveASCWDSHeadOfficeAccountsWithoutLocationIdsTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_remove_head_office_accounts_df = self.spark.createDataFrame(
            Data.remove_head_office_accounts_rows,
            Schemas.remove_head_office_accounts_schema,
        )
        self.returned_df = job.remove_ascwds_head_office_accounts_without_location_ids(
            self.test_remove_head_office_accounts_df,
        )
        self.returned_locations = (
            self.returned_df.select(AWPClean.establishment_id)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def test_keep_ascwds_accounts_when_location_id_is_not_null(self):
        self.assertTrue("1" in self.returned_locations)
        self.assertTrue("2" in self.returned_locations)

    def test_keep_ascwds_accounts_when_location_id_is_null_but_main_service_is_not_head_office(
        self,
    ):
        self.assertTrue("3" in self.returned_locations)

    def test_remove_ascwds_accounts_when_location_id_is_null_and_main_service_is_head_office(
        self,
    ):
        self.assertFalse("4" in self.returned_locations)


class GetAscwdsParentAccounts(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

        # TODO


class FilterToLocationsRelevantToReconciliationTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_filter_to_relevent_df = self.spark.createDataFrame(
            Data.filter_to_relevant_rows, Schemas.filter_to_relevant_schema
        )
        self.first_of_most_recent_month = Data.first_of_most_recent_month
        self.first_of_previous_month = Data.first_of_previous_month
        self.returned_df = job.filter_to_locations_relevant_to_reconcilition_process(
            self.test_filter_to_relevent_df,
            self.first_of_most_recent_month,
            self.first_of_previous_month,
        )
        self.returned_locations = (
            self.returned_df.select(CQCLClean.location_id)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def test_filter_keeps_rows_where_registration_status_is_null(self):
        self.assertTrue("loc_1" in self.returned_locations)
        self.assertTrue("loc_2" in self.returned_locations)
        self.assertTrue("loc_3" in self.returned_locations)
        self.assertTrue("loc_4" in self.returned_locations)
        self.assertTrue("loc_5" in self.returned_locations)
        self.assertTrue("loc_6" in self.returned_locations)
        self.assertTrue("loc_7" in self.returned_locations)
        self.assertTrue("loc_8" in self.returned_locations)

    def test_filter_removes_rows_where_registration_status_is_registered(self):
        self.assertFalse("loc_9" in self.returned_locations)
        self.assertFalse("loc_10" in self.returned_locations)
        self.assertFalse("loc_11" in self.returned_locations)
        self.assertFalse("loc_12" in self.returned_locations)
        self.assertFalse("loc_13" in self.returned_locations)
        self.assertFalse("loc_14" in self.returned_locations)
        self.assertFalse("loc_15" in self.returned_locations)
        self.assertFalse("loc_16" in self.returned_locations)

    def test_filter_keeps_rows_where_registration_status_is_deregistered_and_date_is_within_previous_month(
        self,
    ):
        self.assertTrue("loc_17" in self.returned_locations)
        self.assertTrue("loc_18" in self.returned_locations)
        self.assertTrue("loc_19" in self.returned_locations)
        self.assertTrue("loc_20" in self.returned_locations)

    def test_filter_keeps_rows_where_registration_status_is_deregistered_and_date_is_before_first_of_current_month_and_parents_or_singles_and_subs_is_parent(
        self,
    ):
        self.assertTrue("loc_21" in self.returned_locations)

    def test_filter_removes_rows_where_registration_status_is_deregistered_and_date_is_before_first_of_current_month_and_parents_or_singles_and_subs_is_singles_and_subs(
        self,
    ):
        self.assertFalse("loc_22" in self.returned_locations)

    def test_filter_removes_rows_where_registration_status_is_deregistered_and_date_is_first_of_current_month_or_greater(
        self,
    ):
        self.assertFalse("loc_23" in self.returned_locations)
        self.assertFalse("loc_24" in self.returned_locations)
