import unittest
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    ExtractRegisteredManagerNamesData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    ExtractRegisteredManagerNamesSchema as Schemas,
)

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names"
)


class ExtractRegisteredManagerNamesTests(unittest.TestCase):
    @patch(f"{PATCH_PATH}.add_registered_manager_names")
    @patch(f"{PATCH_PATH}.select_and_create_full_name")
    @patch(f"{PATCH_PATH}.explode_contacts_information")
    def test_extract_registered_manager_names_calls_expected_functions(
        self,
        mock_explode_contacts_information: Mock,
        mock_select_and_create_full_name: Mock,
        mock_add_registered_manager_names: Mock,
    ):
        # GIVEN
        mock_input_df = MagicMock(name="input_df")
        mock_exploded_df = MagicMock(name="exploded_df")
        mock_names_df = MagicMock(name="names_df")
        mock_final_df = MagicMock(name="final_df")

        mock_explode_contacts_information.return_value = mock_exploded_df
        mock_select_and_create_full_name.return_value = mock_names_df
        mock_add_registered_manager_names.return_value = mock_final_df

        # WHEN
        returned_df = job.extract_registered_manager_names(mock_input_df)

        # THEN
        mock_explode_contacts_information.assert_called_once_with(mock_input_df)
        mock_select_and_create_full_name.assert_called_once_with(mock_exploded_df)
        mock_add_registered_manager_names.assert_called_once_with(
            mock_input_df, mock_names_df
        )

        assert returned_df == mock_final_df


class ExplodeContactsInformationTests(unittest.TestCase):
    def test_single_contact(self):
        # GIVEN
        #   Data where location has one activity with one contact
        input_df = pl.DataFrame(
            data=Data.explode_contacts_information_when_single_contact,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_df = job.explode_contacts_information(input_df)

        # THEN
        #   The returned dataframe should have a new column with the contact flattened
        expected_df = pl.DataFrame(
            data=Data.expected_explode_contacts_information_when_single_contact,
            schema=Schemas.expected_explode_contacts_information_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(1, returned_df.shape[0])

    def test_multiple_activities(self):
        # GIVEN
        #   Data where location has multiple activities
        input_df = pl.DataFrame(
            data=Data.explode_contacts_information_when_multiple_activities,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_df = job.explode_contacts_information(input_df)

        # THEN
        #   The returned dataframe should have a new column with all contacts flattened
        expected_df = pl.DataFrame(
            data=Data.expected_explode_contacts_information_when_multiple_activities,
            schema=Schemas.expected_explode_contacts_information_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(4, returned_df.shape[0])

    def test_multiple_contacts_per_activity(self):
        # GIVEN
        #   Data where location has multiple contacts for each activity
        input_df = pl.DataFrame(
            data=Data.explode_contacts_information_when_multiple_contacts_per_activity,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_df = job.explode_contacts_information(input_df)

        # THEN
        #   The returned dataframe should have a new column with all contacts flattened
        expected_df = pl.DataFrame(
            data=Data.expected_explode_contacts_information_when_multiple_contacts_per_activity,
            schema=Schemas.expected_explode_contacts_information_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(4, returned_df.shape[0])

    def test_multiple_activities_and_multiple_contacts_per_activity(
        self,
    ):
        # GIVEN
        #   Data where location has multiple activities and multiple contacts for each activity
        input_df = pl.DataFrame(
            data=Data.explode_contacts_information_when_multiple_activities_and_multple_contacts_per_activity,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_df = job.explode_contacts_information(input_df)

        # THEN
        #   The returned dataframe should have a new column with all contacts flattened
        expected_df = pl.DataFrame(
            data=Data.expected_explode_contacts_information_when_multiple_activities_and_multple_contacts_per_activity,
            schema=Schemas.expected_explode_contacts_information_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(4, returned_df.shape[0])

    def test_contains_empty_contacts(self):
        # GIVEN
        #   Data where location contains activities without any contacts
        input_df = pl.DataFrame(
            data=Data.explode_contacts_information_when_contains_empty_contacts,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_df = job.explode_contacts_information(input_df)

        # THEN
        #   The returned dataframe should have a new column with all non-null contacts flattened
        expected_df = pl.DataFrame(
            data=Data.expected_explode_contacts_information_when_contains_empty_contacts,
            schema=Schemas.expected_explode_contacts_information_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(1, returned_df.shape[0])


class SelectAndCreateFullNameTests(unittest.TestCase):
    def test_when_given_and_family_name_both_populated_returns_full_name(self):
        # GIVEN
        #   Data where contact has both given and family name populated
        input_df = pl.DataFrame(
            data=Data.select_and_create_full_name_when_given_and_family_name_both_populated,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_df = job.select_and_create_full_name(input_df)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_df = pl.DataFrame(
            data=Data.expected_select_and_create_full_name_when_given_and_family_name_both_populated,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned full names should be correctly concatenated
        self.assertEqual(
            "Name Surname",
            returned_df["contacts_full_name"][0],
        )

    def test_when_names_partially_completed_returns_null_name(self):
        # GIVEN
        #   Data where contacts only have either given or family name populated
        input_df = pl.DataFrame(
            data=Data.select_and_create_full_name_when_given_or_family_name_or_null,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_df = job.select_and_create_full_name(input_df)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_df = pl.DataFrame(
            data=Data.expected_select_and_create_full_name_when_given_or_family_name_or_null,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned full name should be null
        self.assertEqual([None, None], returned_df["contacts_full_name"].to_list())

    def test_when_no_contacts_returns_null_name(self):
        # GIVEN
        #   Data where location has no contacts
        input_df = pl.DataFrame(
            data=Data.select_and_create_full_name_without_contact,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_df = job.select_and_create_full_name(input_df)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_df = pl.DataFrame(
            data=Data.expected_select_and_create_full_name_without_contact,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
        #   The returned full name should be null
        self.assertEqual(None, returned_df["contacts_full_name"][0])


class AddRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.input_df = pl.DataFrame(
            data=Data.initial_df,
            schema=Schemas.initial_df_schema,
        )

    def test_with_one_unique_name_per_location(self):
        # GIVEN
        #   Original CQC DataFrame
        input_df = self.input_df
        #   Data where each location has one unique contact name
        contact_names_df = pl.DataFrame(
            data=Data.registered_manager_names_without_duplicates,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_df = job.add_registered_manager_names(input_df, contact_names_df)

        # THEN
        #   The returned dataframe should have a new column with a list of one unique full names per location and import date
        expected_df = pl.DataFrame(
            data=Data.expected_add_registered_manager_names_without_duplicates,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)
        self.assertEqual(
            [
                ["Name Surname_1"],
                ["Name Surname_2"],
                ["Name Surname_3"],
                ["Name Surname_4"],
            ],
            returned_df["registered_manager_names"].to_list(),
        )

    def test_with_duplicate_names_returns_only_unique_names_per_location(self):
        # GIVEN
        #   Original CQC DataFrame
        input_df = self.input_df
        #   Data where locations have duplicate contact names
        contact_names_df = pl.DataFrame(
            data=Data.registered_manager_names_with_duplicates,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_df = job.add_registered_manager_names(input_df, contact_names_df)

        # THEN
        #   The returned dataframe should have a new column with a list of one unique full names per location and import date
        expected_df = pl.DataFrame(
            data=Data.expected_add_registered_manager_names_with_duplicates,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)
        self.assertEqual(
            [
                ["Name Surname_1"],
                ["Name Surname_1"],
                ["Name Surname_2"],
                ["Name Surname_2"],
            ],
            returned_df["registered_manager_names"].to_list(),
        )

    def test_when_multiple_unique_managers_per_location_returns_sorted_list_of_all_names(
        self,
    ):
        # GIVEN
        #   Original CQC DataFrame
        input_df = self.input_df
        #   Data where locations have multiple differing contact names in the same import date
        contact_names_df = pl.DataFrame(
            data=Data.registered_manager_names_with_locations_with_multiple_managers,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_df = job.add_registered_manager_names(input_df, contact_names_df)

        # THEN
        #   The returned dataframe should have a new column with a list of multiple unique full names per location and import date
        expected_df = pl.DataFrame(
            data=Data.expected_registered_manager_names_with_locations_with_multiple_managers,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)
        self.assertEqual(
            [
                ["Name Surname_1"],
                ["Name Surname_1", "Name Surname_2"],
                ["Name Surname_1", "Name Surname_3"],
                ["Name Surname_1", "Name Surname_2", "Name Surname_3"],
            ],
            returned_df["registered_manager_names"].to_list(),
        )

    def test_when_no_contact_names_returns_null_value(self):
        # GIVEN
        #   Original CQC DataFrame
        input_df = self.input_df
        #   Data where locations do not have contact names returns null value
        contact_names_df = pl.DataFrame(
            data=Data.registered_manager_names_with_locations_without_contact_names,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_df = job.add_registered_manager_names(input_df, contact_names_df)

        # THEN
        #   The returned dataframe should have a new column with a list of multiple unique full names per location and import date
        expected_df = pl.DataFrame(
            data=Data.expected_registered_manager_names_with_locations_without_contact_names,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)
        self.assertEqual(
            [["Name Surname"], None, None, None],
            returned_df["registered_manager_names"].to_list(),
        )
