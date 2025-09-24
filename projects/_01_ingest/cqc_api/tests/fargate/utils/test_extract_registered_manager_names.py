import unittest
from unittest.mock import MagicMock, Mock, patch

import projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names as job

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names"
)


class ExtractRegisteredManagerNamesTests(unittest.TestCase):
    @patch(f"{PATCH_PATH}.create_registered_manager_names")
    @patch(f"{PATCH_PATH}.extract_contacts")
    def test_extract_registered_manager_names_calls_expected_functions(
        self,
        mock_extract_contacts: Mock,
        mock_create_registered_manager_names: Mock,
    ):
        # GIVEN
        mock_input_df = MagicMock(name="input_df")
        mock_extracted_df = MagicMock(name="extracted_df")
        mock_final_df = MagicMock(name="final_df")

        mock_extract_contacts.return_value = mock_extracted_df
        mock_create_registered_manager_names.return_value = mock_final_df

        mock_return_df = MagicMock(name="returned_df")
        mock_final_df.drop.return_value = mock_return_df

        # WHEN
        returned_df = job.extract_registered_manager_names(mock_input_df)

        # THEN
        mock_extract_contacts.assert_called_once_with(mock_input_df)
        mock_create_registered_manager_names.assert_called_once_with(mock_extracted_df)

        assert returned_df == mock_return_df
