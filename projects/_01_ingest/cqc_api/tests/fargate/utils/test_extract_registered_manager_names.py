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
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(1, returned_lf.collect().shape[0])

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
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(4, returned_lf.collect().shape[0])

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
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(4, returned_lf.collect().shape[0])

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
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(4, returned_lf.collect().shape[0])

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
        #   The returned dataframe should have the expected number of exploded rows
        self.assertEqual(1, returned_lf.collect().shape[0])


class SelectAndCreateFullNameTests(unittest.TestCase):
    def test_when_given_and_family_name_both_populated_returns_full_name(self):
        # GIVEN
        #   Data where contact has both given and family name populated
        input_lf = pl.LazyFrame(
            data=Data.select_and_create_full_name_when_given_and_family_name_both_populated,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_lf = job.select_and_create_full_name(input_lf)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_lf = pl.LazyFrame(
            data=Data.expected_select_and_create_full_name_when_given_and_family_name_both_populated,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
        #   The returned full names should be correctly concatenated
        self.assertEqual(
            "Name Surname",
            returned_lf.collect()["contacts_full_name"][0],
        )

    def test_when_names_partially_completed_returns_only_complete_names(self):
        # GIVEN
        #   Data where contacts only have either given or family name populated
        input_lf = pl.LazyFrame(
            data=Data.select_and_create_full_name_when_given_or_family_name_or_null,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_lf = job.select_and_create_full_name(input_lf)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_lf = pl.LazyFrame(
            data=Data.expected_select_and_create_full_name_when_given_or_family_name_or_null,
            schema=Schemas.expected_select_and_create_full_name_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_no_contacts_returns_empty_lf(self):
        # GIVEN
        #   Data where location has no contacts
        input_lf = pl.LazyFrame(
            data=Data.select_and_create_full_name_without_contact,
            schema=Schemas.select_and_create_full_name_schema,
        )

        # WHEN
        returned_lf = job.select_and_create_full_name(input_lf)

        # THEN
        #   The returned dataframe should have a location_id, import date and full name column
        expected_lf = pl.LazyFrame(
            data=[],
            schema=Schemas.expected_select_and_create_full_name_schema,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AddRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.input_lf = pl.LazyFrame(
            data=Data.add_registered_manager_names_full_lf,
            schema=Schemas.add_registered_manager_names_full_lf_schema,
        )

    def test_with_one_unique_name_per_location(self):
        # GIVEN
        #   Original CQC DataFrame
        input_lf = self.input_lf
        #   Data where each location has one unique contact name
        contact_names_lf = pl.LazyFrame(
            data=Data.registered_manager_names_without_duplicates,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_lf = job.add_registered_manager_names(input_lf, contact_names_lf)

        # THEN
        #   The returned dataframe should have a new column with a list of one unique full names per location and import date
        expected_lf = pl.LazyFrame(
            data=Data.expected_add_registered_manager_names_without_duplicates,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
        self.assertEqual(
            [
                ["Name Surname_1"],
                ["Name Surname_2"],
                ["Name Surname_3"],
                ["Name Surname_4"],
            ],
            returned_lf.collect()["registered_manager_names"].to_list(),
        )

    def test_with_duplicate_names_returns_only_unique_names_per_location(self):
        # GIVEN
        #   Original CQC DataFrame
        input_lf = self.input_lf
        #   Data where locations have duplicate contact names
        contact_names_lf = pl.LazyFrame(
            data=Data.registered_manager_names_with_duplicates,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_lf = job.add_registered_manager_names(input_lf, contact_names_lf)

        # THEN
        #   The returned dataframe should have a new column with a list of one unique full names per location and import date
        expected_lf = pl.LazyFrame(
            data=Data.expected_add_registered_manager_names_with_duplicates,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
        self.assertEqual(
            [
                ["Name Surname_1"],
                ["Name Surname_1"],
                ["Name Surname_2"],
                ["Name Surname_2"],
            ],
            returned_lf.collect()["registered_manager_names"].to_list(),
        )

    def test_when_multiple_unique_managers_per_location_returns_sorted_list_of_all_names(
        self,
    ):
        # GIVEN
        #   Original CQC DataFrame
        input_lf = self.input_lf
        #   Data where locations have multiple differing contact names in the same import date
        contact_names_lf = pl.LazyFrame(
            data=Data.registered_manager_names_with_locations_with_multiple_managers,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_lf = job.add_registered_manager_names(input_lf, contact_names_lf)

        # THEN
        #   The returned dataframe should have a new column with a list of multiple unique full names per location and import date
        expected_lf = pl.LazyFrame(
            data=Data.expected_registered_manager_names_with_locations_with_multiple_managers,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
        self.assertEqual(
            [
                ["Name Surname_1"],
                ["Name Surname_1", "Name Surname_2"],
                ["Name Surname_1", "Name Surname_3"],
                ["Name Surname_1", "Name Surname_2", "Name Surname_3"],
            ],
            returned_lf.collect()["registered_manager_names"].to_list(),
        )

    def test_when_no_contact_names_returns_null_value(self):
        # GIVEN
        #   Original CQC DataFrame
        input_lf = self.input_lf
        #   Data where locations do not have contact names returns null value
        contact_names_lf = pl.LazyFrame(
            data=Data.registered_manager_names_with_locations_without_contact_names,
            schema=Schemas.registered_manager_names_schema,
        )

        # WHEN
        returned_lf = job.add_registered_manager_names(input_lf, contact_names_lf)

        # THEN
        #   The returned dataframe should have a new column with a list of multiple unique full names per location and import date
        expected_lf = pl.LazyFrame(
            data=Data.expected_registered_manager_names_with_locations_without_contact_names,
            schema=Schemas.expected_add_registered_manager_names_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
        self.assertEqual(
            [["Name Surname"], None, None, None],
            returned_lf.collect()["registered_manager_names"].to_list(),
        )
