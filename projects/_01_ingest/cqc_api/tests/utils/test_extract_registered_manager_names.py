import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.utils.extract_registered_manager_names as job
from tests.test_file_data import ExtractRegisteredManagerNamesData as Data
from tests.test_file_schemas import ExtractRegisteredManagerNamesSchema as Schemas
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLCleaned,
)

PATCH_PATH: str = "projects._01_ingest.cqc_api.utils.extract_registered_manager_names"


class ExtractRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()


class MainTests(ExtractRegisteredManagerNamesTests):
    def setUp(self) -> None:
        super().setUp()

        self.extract_registered_manager_df = self.spark.createDataFrame(
            Data.extract_registered_manager_rows,
            Schemas.extract_registered_manager_schema,
        )

    @patch(f"{PATCH_PATH}.join_names_column_into_original_df")
    @patch(f"{PATCH_PATH}.group_and_collect_names")
    @patch(f"{PATCH_PATH}.select_and_create_full_name")
    @patch(f"{PATCH_PATH}.extract_contacts_information")
    def test_extract_registered_manager_names_calls_all_functions(
        self,
        extract_contacts_information_mock: Mock,
        select_and_create_full_name_mock: Mock,
        group_and_collect_names_mock: Mock,
        join_names_column_into_original_df_mock: Mock,
    ):
        job.extract_registered_manager_names(self.extract_registered_manager_df)

        extract_contacts_information_mock.assert_called_once()
        select_and_create_full_name_mock.assert_called_once()
        group_and_collect_names_mock.assert_called_once()
        join_names_column_into_original_df_mock.assert_called_once()

    def test_extract_registered_manager_names_returns_the_same_number_of_rows(self):
        returned_df = job.extract_registered_manager_names(
            self.extract_registered_manager_df
        )
        self.assertEqual(
            self.extract_registered_manager_df.count(), returned_df.count()
        )


class ExtractContactsInformationTests(ExtractRegisteredManagerNamesTests):
    def setUp(self) -> None:
        super().setUp()

    def test_extract_contacts_information_returns_one_row_per_activity_with_a_contact(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.extract_contacts_information_two_activities_one_contact_each_rows,
            Schemas.extract_contacts_information_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_extract_contacts_information_two_activities_one_contact_each_rows,
            Schemas.expected_extract_contacts_information_schema,
        )
        returned_df = job.extract_contacts_information(test_df)
        self.assertEqual(
            expected_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.contacts_exploded
            ).collect(),
            returned_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.contacts_exploded
            ).collect(),
        )

    def test_extract_contacts_information_returns_one_row_per_contact_when_mutliple_contacts(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.extract_contacts_information_one_activity_two_contacts_rows,
            Schemas.extract_contacts_information_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_extract_contacts_information_one_activity_two_contacts_rows,
            Schemas.expected_extract_contacts_information_schema,
        )
        returned_df = job.extract_contacts_information(test_df)
        self.assertEqual(
            expected_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.contacts_exploded
            ).collect(),
            returned_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.contacts_exploded
            ).collect(),
        )

    def test_extract_contacts_information_doesnt_return_a_row_if_no_contacts(self):
        test_df = self.spark.createDataFrame(
            Data.extract_contacts_information_two_activities_but_only_one_with_contacts_rows,
            Schemas.extract_contacts_information_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_extract_contacts_information_two_activities_but_only_one_with_contacts_rows,
            Schemas.expected_extract_contacts_information_schema,
        )
        returned_df = job.extract_contacts_information(test_df)
        self.assertEqual(
            expected_df.collect(),
            returned_df.collect(),
        )


class SelectAndCreateFullNameTests(ExtractRegisteredManagerNamesTests):
    def setUp(self) -> None:
        super().setUp()

    def test_select_and_create_full_name_returns_expected_columns(self):
        test_df = self.spark.createDataFrame(
            Data.select_and_create_full_name_rows,
            Schemas.select_and_create_full_name_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_select_and_create_full_name_rows,
            Schemas.expected_select_and_create_full_name_schema,
        )
        returned_df = job.select_and_create_full_name(test_df)
        self.assertEqual(expected_df.columns, returned_df.columns)

    def test_select_and_create_full_name_returns_expected_rows(self):
        test_df = self.spark.createDataFrame(
            Data.select_and_create_full_name_rows,
            Schemas.select_and_create_full_name_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_select_and_create_full_name_rows,
            Schemas.expected_select_and_create_full_name_schema,
        )
        returned_df = job.select_and_create_full_name(test_df)
        self.assertEqual(
            expected_df.sort(CQCLCleaned.location_id).collect(),
            returned_df.sort(CQCLCleaned.location_id).collect(),
        )


class GroupAndCollectNamesTests(ExtractRegisteredManagerNamesTests):
    def setUp(self) -> None:
        super().setUp()

    def test_group_and_collect_names_removes_duplicate_names(self):
        test_df = self.spark.createDataFrame(
            Data.group_and_collect_names_with_duplicate_names_rows,
            Schemas.group_and_collect_names_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_group_and_collect_names_with_duplicate_names_rows,
            Schemas.expected_group_and_collect_names_schema,
        )
        returned_df = job.group_and_collect_names(test_df)
        self.assertEqual(
            expected_df.collect(),
            returned_df.collect(),
        )

    def test_group_and_collect_names_groups_by_locationid_and_import_date_correctly(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.group_and_collect_names_with_different_ids_and_dates_rows,
            Schemas.group_and_collect_names_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_group_and_collect_names_with_different_ids_and_dates_rows,
            Schemas.expected_group_and_collect_names_schema,
        )
        returned_df = job.group_and_collect_names(test_df)
        self.assertEqual(
            expected_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.cqc_location_import_date
            ).collect(),
            returned_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.cqc_location_import_date
            ).collect(),
        )


class JoinNamesColumnIntoOriginalDfTests(ExtractRegisteredManagerNamesTests):
    def setUp(self) -> None:
        super().setUp()

    def test_join_names_column_into_original_df_returns_expected_df(
        self,
    ):
        original_df = self.spark.createDataFrame(
            Data.original_test_rows,
            Schemas.original_test_schema,
        )
        registered_manager_names_df = self.spark.createDataFrame(
            Data.registered_manager_names_rows,
            Schemas.registered_manager_names_schema,
        )
        expected_joined_df = self.spark.createDataFrame(
            Data.expected_join_with_original_rows,
            Schemas.expected_join_with_original_schema,
        )
        returned_joined_df = job.join_names_column_into_original_df(
            original_df, registered_manager_names_df
        )
        self.assertEqual(
            expected_joined_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.cqc_location_import_date
            ).collect(),
            returned_joined_df.sort(
                CQCLCleaned.location_id, CQCLCleaned.cqc_location_import_date
            ).collect(),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
