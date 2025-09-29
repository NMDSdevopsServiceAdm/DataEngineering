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


class SelectAndCreateFullNameTests(unittest.TestCase):
    def test_select_and_create_full_name_returns_expected_dataframe(self):
        # GIVEN
        #   Data where location has contacts as a flattened struct column
        input_df = pl.DataFrame(
            data=Data.select_and_create_full_name,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_df = job.select_and_create_full_name(input_df)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_df = pl.DataFrame(
            data=Data.expected_select_and_create_full_name,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)


class AddRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.input_df = pl.DataFrame(
            data=Data.add_registered_manager_names_full_df,
            schema=Schemas.add_registered_manager_names_full_df_schema,
        )

    # def test_join_keys_applied(self):
    #     # GIVEN
    #     #   Data where location has contacts as a flattened struct column

    #     contact_names_df = pl.DataFrame(
    #         data=Data.add_registered_manager_names_contact_names,
    #         schema=Schemas.add_registered_manager_names_contact_names_schema,
    #     )

    #     # WHEN
    #     returned_df = job.add_registered_manager_names(self.input_df, contact_names_df)

    #     # THEN
    #     #   The returned dataframe should have a new column with unique full names per location and import date
    #     expected_df = pl.DataFrame(
    #         data=Data.expected_add_registered_manager_names,
    #         schema=Schemas.expected_add_registered_manager_names_schema,
    #     )

    #     pl_testing.assert_frame_equal(returned_df, expected_df)
