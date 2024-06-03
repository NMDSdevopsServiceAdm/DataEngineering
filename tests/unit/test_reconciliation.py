import unittest
import warnings
from datetime import date
from unittest.mock import Mock, patch

import jobs.reconciliation as job
from utils import utils

from tests.test_file_data import ReconciliationData as Data
from tests.test_file_schemas import ReconciliationSchema as Schemas
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
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
    @patch("utils.utils.filter_df_to_maximum_value_in_column")
    def test_prepare_latest_cleaned_ascwds_workforce_data_runs(
        self,
        filter_df_to_maximum_value_in_column_patch: Mock,
        add_parents_or_singles_and_subs_col_to_df_patch: Mock,
        get_ascwds_parent_accounts_patch: Mock,
        filter_to_cqc_registration_type_only_patch: Mock,
        remove_ascwds_head_office_accounts_without_location_ids_patch: Mock,
    ):
        job.prepare_latest_cleaned_ascwds_workforce_data(
            self.test_clean_ascwds_workplace_df,
        )

        self.assertEqual(filter_df_to_maximum_value_in_column_patch.call_count, 1)
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
        self.test_get_parents_df = self.spark.createDataFrame(
            Data.get_ascwds_parent_accounts_rows,
            Schemas.get_ascwds_parent_accounts_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_get_ascwds_parent_accounts_rows,
            Schemas.expected_get_ascwds_parent_accounts_schema,
        )
        self.returned_df = job.get_ascwds_parent_accounts(self.test_get_parents_df)

    def test_get_ascwds_parent_accounts_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class JoinCQCLocationDataIntoASCWDSWorkplaceDataframe(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_df = self.spark.createDataFrame(
            Data.cqc_data_for_join_rows, Schemas.cqc_data_for_join_schema
        )
        self.test_ascwds_df = self.spark.createDataFrame(
            Data.ascwds_data_for_join_rows, Schemas.ascwds_data_for_join_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_data_for_join_rows, Schemas.expected_data_for_join_schema
        )
        self.returned_df = job.join_cqc_location_data_into_ascwds_workplace_df(
            self.test_ascwds_df, self.test_cqc_df
        )
        self.expected_df.show()
        self.returned_df.show()

    def test_join_cqc_locations_data_into_ascwds_workplace_df_returns_correct_values(
        self,
    ):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


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


class AddSinglesAndSubDescriptionColumnTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_add_singles_and_subs_description_df = self.spark.createDataFrame(
            Data.add_singles_and_subs_description_rows,
            Schemas.add_singles_and_subs_description_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_singles_and_subs_description_rows,
            Schemas.expected_singles_and_subs_description_schema,
        )
        self.returned_df = job.add_singles_and_sub_description_column(
            self.test_add_singles_and_subs_description_df,
        )

    def test_add_singles_and_subs_description_column_adds_a_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = (
            len(self.test_add_singles_and_subs_description_df.columns) + 1
        )
        self.assertEqual(returned_columns, expected_columns)

    def test_add_singles_and_subs_description_column_adds_a_column_with_expected_values(
        self,
    ):
        returned_data = self.returned_df.sort(CQCL.location_id).collect()
        expected_data = self.expected_df.sort(CQCL.location_id).collect()
        self.assertEqual(returned_data, expected_data)


class CreateMissingColumnsRequiredForOutputTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_create_missing_columns_df = self.spark.createDataFrame(
            Data.create_missing_columns_rows,
            Schemas.create_missing_columns_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_missing_columns_rows,
            Schemas.expected_create_missing_columns_schema,
        )
        self.returned_df = job.create_missing_columns_required_for_output(
            self.test_create_missing_columns_df,
        )
        self.returned_df.show()
        self.returned_df.printSchema()

    def test_create_missing_columns_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_create_missing_columns_returns_expected_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

    def test_create_missing_columns_returns_expected_values_in_new_columns(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(
            returned_data[0][ReconColumn.nmds], expected_data[0][ReconColumn.nmds]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.workplace_id],
            expected_data[0][ReconColumn.workplace_id],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.requester_name],
            expected_data[0][ReconColumn.requester_name],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.requester_name_2],
            expected_data[0][ReconColumn.requester_name_2],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.status], expected_data[0][ReconColumn.status]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.technician],
            expected_data[0][ReconColumn.technician],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.manual_call_log],
            expected_data[0][ReconColumn.manual_call_log],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.mode], expected_data[0][ReconColumn.mode]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.priority],
            expected_data[0][ReconColumn.priority],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.category],
            expected_data[0][ReconColumn.category],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.sub_category],
            expected_data[0][ReconColumn.sub_category],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.is_requester_named],
            expected_data[0][ReconColumn.is_requester_named],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.security_question],
            expected_data[0][ReconColumn.security_question],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.website], expected_data[0][ReconColumn.website]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.item], expected_data[0][ReconColumn.item]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.phone], expected_data[0][ReconColumn.phone]
        )


class FinalColumnSelectionTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_final_column_selection_df = self.spark.createDataFrame(
            Data.final_column_selection_rows,
            Schemas.final_column_selection_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_final_column_selection_rows,
            Schemas.expected_final_column_selection_schema,
        )
        self.returned_df = job.final_column_selection(
            self.test_final_column_selection_df,
        )

    def test_final_column_selection_contains_correct_columns(self):
        print(self.returned_df.columns)
        print(self.expected_df.columns)
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_final_column_selection_sorts_data_correctly(self):
        returned_data = self.returned_df.select(
            ReconColumn.nmds, ReconColumn.description
        ).collect()
        expected_data = self.expected_df.select(
            ReconColumn.nmds, ReconColumn.description
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_final_column_selection_returns_expected_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())


class AddSubjectColumnTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.add_subject_column_rows,
            Schemas.add_subject_column_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_subject_column_rows,
            Schemas.expected_add_subject_column_schema,
        )
        self.returned_df = job.add_subject_column(self.test_df, "test_subject")

    def test_add_subject_column_adds_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.test_df.columns) + 1
        self.assertEqual(returned_columns, expected_columns)

    def test_add_subject_column_adds_column_with_correct_value(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class JoinArrayOfNmdsIdsTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

        self.new_issues_df = self.spark.createDataFrame(
            Data.new_issues_rows,
            Schemas.new_issues_schema,
        )
        self.unique_df = self.spark.createDataFrame(
            Data.unique_rows,
            Schemas.unique_schema,
        )
        self.new_column = Data.new_column
        self.returned_df = job.join_array_of_nmdsids_into_parent_account_df(
            self.new_issues_df, self.new_column, self.unique_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_join_array_of_nmdsids_rows,
            Schemas.expected_join_array_of_nmdsids_schema,
        )

    def test_join_array_of_nmdsids_returns_one_row_per_org_id(self):
        expected_rows = self.unique_df.count()
        returned_rows = self.returned_df.count()
        self.assertEqual(returned_rows, expected_rows)

    def test_join_array_of_nmdsids_adds_one_column(self):
        expected_columns = len(self.unique_df.columns) + 1
        returned_columns = len(self.returned_df.columns)
        self.assertEqual(returned_columns, expected_columns)

    def test_join_array_of_nmdsids_adds_new_column_as_string_type(
        self,
    ):
        expected_data_type = dict(self.expected_df.dtypes)[self.new_column]
        returned_data_type = dict(self.returned_df.dtypes)[self.new_column]
        self.assertEqual(returned_data_type, expected_data_type)

    def test_join_array_of_nmdsids_groups_nmdsids_correctly(self):
        expected_data = self.expected_df.sort(AWPClean.organisation_id).collect()
        returned_data = self.returned_df.sort(AWPClean.organisation_id).collect()
        self.assertEqual(expected_data, returned_data)


class CreateDescriptionColumnForParentAccountsTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.df = self.spark.createDataFrame(
            Data.create_parents_description_rows,
            Schemas.create_parents_description_schema,
        )
        self.returned_df = job.create_description_column_for_parent_accounts(self.df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_parents_description_rows,
            Schemas.expected_create_parents_description_schema,
        )
        self.returned_data = self.returned_df.collect()
        self.expected_data = self.expected_df.collect()

    def test_create_description_column_for_parent_accounts_adds_one_column(self):
        expected_columns = len(self.df.columns) + 1
        returned_columns = len(self.returned_df.columns)
        self.assertEqual(returned_columns, expected_columns)

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_null_old_is_null_and_missing_is_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[0][ReconColumn.description],
            self.expected_data[0][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_null_old_is_null_and_missing_is_not_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[1][ReconColumn.description],
            self.expected_data[1][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_null_old_is_not_null_and_missing_is_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[2][ReconColumn.description],
            self.expected_data[2][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_null_old_is_not_null_and_missing_is_not_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[3][ReconColumn.description],
            self.expected_data[3][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_not_null_old_is_null_and_missing_is_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[4][ReconColumn.description],
            self.expected_data[4][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_not_null_old_is_null_and_missing_is_not_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[5][ReconColumn.description],
            self.expected_data[5][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_not_null_old_is_not_null_and_missing_is_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[6][ReconColumn.description],
            self.expected_data[6][ReconColumn.description],
        )

    def test_create_description_column_for_parent_accounts_formats_sstring_correctly_when_new_is_not_null_old_is_not_null_and_missing_is_not_null(
        self,
    ):
        self.assertEqual(
            self.returned_data[7][ReconColumn.description],
            self.expected_data[7][ReconColumn.description],
        )
