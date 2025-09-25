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
    pass
    #     @patch(f"{PATCH_PATH}.create_registered_manager_names")
    #     @patch(f"{PATCH_PATH}.extract_contacts")
    #     def test_extract_registered_manager_names_calls_expected_functions(
    #         self,
    #         mock_extract_contacts: Mock,
    #         mock_create_registered_manager_names: Mock,
    #     ):
    #         # GIVEN
    #         mock_input_df = MagicMock(name="input_df")
    #         mock_extracted_df = MagicMock(name="extracted_df")
    #         mock_final_df = MagicMock(name="final_df")

    #         mock_extract_contacts.return_value = mock_extracted_df
    #         mock_create_registered_manager_names.return_value = mock_final_df

    #         mock_return_df = MagicMock(name="returned_df")
    #         mock_final_df.drop.return_value = mock_return_df

    #         # WHEN
    #         returned_df = job.extract_registered_manager_names(mock_input_df)

    #         # THEN
    #         mock_extract_contacts.assert_called_once_with(mock_input_df)
    #         mock_create_registered_manager_names.assert_called_once_with(mock_extracted_df)

    #         assert returned_df == mock_return_df


# class ExtractContactsTests(unittest.TestCase):
#     def test_single_contact(self):
#         # GIVEN
#         #   Data where location has one activity with one contact
#         input_df = pl.DataFrame(
#             data=Data.extract_contacts_when_single_contact,
#             schema=Schemas.extract_contacts_schema,
#         )

#         # WHEN
#         returned_df = job.extract_contacts(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with the contact flattened
#         expected_df = pl.DataFrame(
#             data=Data.expected_extract_contacts_when_single_contact,
#             schema=Schemas.expected_extract_contacts_schema,
#         )

#         pl_testing.assert_frame_equal(returned_df, expected_df)

#     def test_multiple_activities(self):
#         # GIVEN
#         #   Data where location has multiple activities
#         input_df = pl.DataFrame(
#             data=Data.extract_contacts_when_multiple_activities,
#             schema=Schemas.extract_contacts_schema,
#         )

#         # WHEN
#         returned_df = job.extract_contacts(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with all contacts flattened
#         expected_df = pl.DataFrame(
#             data=Data.expected_extract_contacts_when_multiple_activities,
#             schema=Schemas.expected_extract_contacts_schema,
#         )
#         print(returned_df)
#         print(expected_df)

#         pl_testing.assert_frame_equal(returned_df, expected_df)

#     def test_multiple_contacts_per_activity(self):
#         # GIVEN
#         #   Data where location has multiple contacts for each activity
#         input_df = pl.DataFrame(
#             data=Data.extract_contacts_when_multiple_contacts_per_activity,
#             schema=Schemas.extract_contacts_schema,
#         )

#         # WHEN
#         returned_df = job.extract_contacts(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with all contacts flattened
#         expected_df = pl.DataFrame(
#             data=Data.expected_extract_contacts_when_multiple_contacts_per_activity,
#             schema=Schemas.expected_extract_contacts_schema,
#         )

#         pl_testing.assert_frame_equal(returned_df, expected_df)

#     def test_multiple_activities_and_multiple_contacts_per_activity(
#         self,
#     ):
#         # GIVEN
#         #   Data where location has multiple activities and multiple contacts for each activity
#         input_df = pl.DataFrame(
#             data=Data.extract_contacts_when_multiple_activities_and_multple_contacts_per_activity,
#             schema=Schemas.extract_contacts_schema,
#         )

#         # WHEN
#         returned_df = job.extract_contacts(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with all contacts flattened
#         expected_df = pl.DataFrame(
#             data=Data.expected_extract_contacts_when_multiple_activities_and_multple_contacts_per_activity,
#             schema=Schemas.expected_extract_contacts_schema,
#         )
#         print(returned_df)
#         print(expected_df)

#         pl_testing.assert_frame_equal(returned_df, expected_df)

#     def test_no_contacts(self):
#         # GIVEN
#         #   Data where location does not have any contacts for any activity
#         input_df = pl.DataFrame(
#             data=Data.extract_contacts_when_no_contacts,
#             schema=Schemas.extract_contacts_schema,
#         )

#         # WHEN
#         returned_df = job.extract_contacts(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with all contacts flattened
#         expected_df = pl.DataFrame(
#             data=Data.expected_extract_contacts_when_no_contacts,
#             schema=Schemas.expected_extract_contacts_schema,
#         )

#         pl_testing.assert_frame_equal(returned_df, expected_df)


# class CreateRegisteredManagerNamesTests(unittest.TestCase):
#     def test_single_contact(self):
#         # GIVEN
#         #   Data where location has one contact
#         input_df = pl.DataFrame(
#             data=Data.create_registered_manager_names_when_single_contact,
#             schema=Schemas.create_registered_manager_names_schema,
#         )

#         # WHEN
#         returned_df = job.create_registered_manager_names(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with a list of the unique Registered Manager names
#         expected_df = pl.DataFrame(
#             data=Data.expected_create_registered_manager_names_when_single_contact,
#             schema=Schemas.expected_create_registered_manager_names_schema,
#         )

#         pl_testing.assert_frame_equal(returned_df, expected_df)

#     def test_multiple_inner_lists(self):
#         pass

#     def test_multiple_contacts_single_inner_list(self):
#         pass

#     def test_multiple_inner_lists_multiple_contacts(self):
#         # GIVEN
#         #   Data where a location has a mix of inner lists and multiple contacts
#         input_df = pl.DataFrame(
#             data=Data.extract_contacts_when_single_contact,
#             schema=Schemas.create_registered_manager_names_schema,
#         )

#         # WHEN
#         returned_df = job.create_registered_manager_names(input_df)

#         # THEN
#         #   The returned dataframe should have a new column with a list of the unique Registered Manager names
#         expected_df = pl.DataFrame(
#             data=Data.expected_extract_contacts_when_single_contact,
#             schema=Schemas.expected_create_registered_manager_names_schema,
#         )

#         pl_testing.assert_frame_equal(returned_df, expected_df)

#     def test_duplicate_names(self):
#         pass

#     def test_empty_lists(self):
#         pass
