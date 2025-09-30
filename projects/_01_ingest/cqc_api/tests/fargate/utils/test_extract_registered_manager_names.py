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
        mock_input_lf = MagicMock(name="input_lf")
        mock_exploded_lf = MagicMock(name="exploded_lf")
        mock_names_lf = MagicMock(name="names_lf")
        mock_final_lf = MagicMock(name="final_lf")

        mock_explode_contacts_information.return_value = mock_exploded_lf
        mock_select_and_create_full_name.return_value = mock_names_lf
        mock_add_registered_manager_names.return_value = mock_final_lf

        # WHEN
        returned_lf = job.extract_registered_manager_names(mock_input_lf)

        # THEN
        mock_explode_contacts_information.assert_called_once_with(mock_input_lf)
        mock_select_and_create_full_name.assert_called_once_with(mock_exploded_lf)
        mock_add_registered_manager_names.assert_called_once_with(
            mock_input_lf, mock_names_lf
        )

        assert returned_lf == mock_final_lf


class ExplodeContactsInformationTests(unittest.TestCase):
    def test_single_contact(self):
        # GIVEN
        #   Data where location has one activity with one contact
        input_lf = pl.LazyFrame(
            data=Data.explode_contacts_information_when_single_contact,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_lf = job.explode_contacts_information(input_lf)

        # THEN
        #   The returned dataframe should have a new column with the contact flattened
        expected_lf = pl.LazyFrame(
            data=Data.expected_explode_contacts_information_when_single_contact,
            schema=Schemas.expected_explode_contacts_information_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_multiple_activities(self):
        # GIVEN
        #   Data where location has multiple activities
        input_lf = pl.LazyFrame(
            data=Data.explode_contacts_information_when_multiple_activities,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_lf = job.explode_contacts_information(input_lf)

        # THEN
        #   The returned dataframe should have a new column with all contacts flattened
        expected_lf = pl.LazyFrame(
            data=Data.expected_explode_contacts_information_when_multiple_activities,
            schema=Schemas.expected_explode_contacts_information_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_multiple_contacts_per_activity(self):
        # GIVEN
        #   Data where location has multiple contacts for each activity
        input_lf = pl.LazyFrame(
            data=Data.explode_contacts_information_when_multiple_contacts_per_activity,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_lf = job.explode_contacts_information(input_lf)

        # THEN
        #   The returned dataframe should have a new column with all contacts flattened
        expected_lf = pl.LazyFrame(
            data=Data.expected_explode_contacts_information_when_multiple_contacts_per_activity,
            schema=Schemas.expected_explode_contacts_information_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_multiple_activities_and_multiple_contacts_per_activity(
        self,
    ):
        # GIVEN
        #   Data where location has multiple activities and multiple contacts for each activity
        input_lf = pl.LazyFrame(
            data=Data.explode_contacts_information_when_multiple_activities_and_multple_contacts_per_activity,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_lf = job.explode_contacts_information(input_lf)

        # THEN
        #   The returned dataframe should have a new column with all contacts flattened
        expected_lf = pl.LazyFrame(
            data=Data.expected_explode_contacts_information_when_multiple_activities_and_multple_contacts_per_activity,
            schema=Schemas.expected_explode_contacts_information_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_contains_empty_contacts(self):
        # GIVEN
        #   Data where location contains activities without any contacts
        input_lf = pl.LazyFrame(
            data=Data.explode_contacts_information_when_contains_empty_contacts,
            schema=Schemas.explode_contacts_information_schema,
        )

        # WHEN
        returned_lf = job.explode_contacts_information(input_lf)

        # THEN
        #   The returned dataframe should have a new column with all non-null contacts flattened
        expected_lf = pl.LazyFrame(
            data=Data.expected_explode_contacts_information_when_contains_empty_contacts,
            schema=Schemas.expected_explode_contacts_information_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class SelectAndCreateFullNameTests(unittest.TestCase):
    def test_select_and_create_full_name_returns_expected_dataframe(self):
        # GIVEN
        #   Data where location has contacts as a flattened struct column
        input_lf = pl.LazyFrame(
            data=Data.select_and_create_full_name,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_lf = job.select_and_create_full_name(input_lf)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_lf = pl.LazyFrame(
            data=Data.expected_select_and_create_full_name,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AddRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.input_lf = pl.LazyFrame(
            data=Data.add_registered_manager_names_full_lf,
            schema=Schemas.add_registered_manager_names_full_lf_schema,
        )

    # def test_join_keys_applied(self):
    #     # GIVEN
    #     #   Data where location has contacts as a flattened struct column

    #     contact_names_lf = pl.LazyFrame(
    #         data=Data.add_registered_manager_names_contact_names,
    #         schema=Schemas.add_registered_manager_names_contact_names_schema,
    #     )

    #     # WHEN
    #     returned_lf = job.add_registered_manager_names(self.input_lf, contact_names_lf)

    #     # THEN
    #     #   The returned dataframe should have a new column with unique full names per location and import date
    #     expected_lf = pl.LazyFrame(
    #         data=Data.expected_add_registered_manager_names,
    #         schema=Schemas.expected_add_registered_manager_names_schema,
    #     )

    #     pl_testing.assert_frame_equal(returned_lf, expected_lf)
