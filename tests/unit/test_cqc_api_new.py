import unittest
from unittest.mock import Mock, patch, call


from utils import cqc_api_new as cqc

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)


# TODO : test call_api()
# TODO : test get_all_objects()


class CqcApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_url = "test_url"


class TestResponse:
    status_code: int = 500
    content: dict = {}

    def __init__(self, status_code: int, content: dict) -> None:
        self.status_code = status_code
        self.content = content

    def json(self):
        return self.content


class LocationApiTests(CqcApiTests):
    def setUp(self) -> None:
        super().setUp()
        self.location_object_type = "locations"
        self.location_id_examples = ["location_1", "location_2", "location_3"]
        self.test_locations = [
            {CQCL.location_id: self.location_id_examples[0]},
            {CQCL.location_id: self.location_id_examples[1]},
            {CQCL.location_id: self.location_id_examples[2]},
        ]

    @patch("utils.cqc_api_new.call_api")
    def test_get_object_when_getting_location(self, mock_call_api: Mock):
        mock_call_api.return_value = {CQCL.location_id: "location_1"}

        returned_location = cqc.get_object("location_1", self.location_object_type)
        expected_location = {CQCL.location_id: "location_1"}

        self.assertEqual(returned_location, expected_location)

    @patch("utils.cqc_api_new.call_api")
    @patch("utils.cqc_api_new.get_object")
    def test_get_page_objects_when_getting_locations(
        self, mock_get_object: Mock, mock_call_api: Mock
    ):
        mock_call_api.return_value = {self.location_object_type: self.test_locations}

        mock_get_object.side_effect = self.test_locations

        returned_page_objects = cqc.get_page_objects(
            self.test_url, 1, self.location_object_type, CQCL.location_id, "PARTNERCODE"
        )

        expected_page_objects = self.test_locations
        self.assertEqual(returned_page_objects, expected_page_objects)

        mock_call_api.assert_called_once_with(
            self.test_url,
            {"page": 1, "perPage": 500, "partnerCode": "PARTNERCODE"},
            headers_dict={"User-Agent": "SkillsForCare"},
        )

        mock_get_object.assert_has_calls(
            [
                call(self.location_id_examples[0], self.location_object_type),
                call(self.location_id_examples[1], self.location_object_type),
                call(self.location_id_examples[2], self.location_object_type),
            ]
        )


class ProviderApiTests(CqcApiTests):
    def setUp(self) -> None:
        super().setUp()
        self.provider_object_type = "providers"
        self.provider_id_examples = ["provider_1", "provider_2", "provider_3"]
        self.test_providers = [
            {CQCP.provider_id: self.provider_id_examples[0]},
            {CQCP.provider_id: self.provider_id_examples[1]},
            {CQCP.provider_id: self.provider_id_examples[2]},
        ]

    @patch("utils.cqc_api_new.call_api")
    def test_get_object_when_getting_provider(self, mock_call_api: Mock):
        mock_call_api.return_value = {CQCP.provider_id: "provider_1"}

        returned_provider = cqc.get_object("provider_1", self.provider_object_type)
        expected_provider = {CQCP.provider_id: "provider_1"}

        self.assertEqual(returned_provider, expected_provider)

    @patch("utils.cqc_api_new.call_api")
    @patch("utils.cqc_api_new.get_object")
    def test_get_page_objects_when_getting_providers(
        self, mock_get_object: Mock, mock_call_api: Mock
    ):
        mock_call_api.return_value = {self.provider_object_type: self.test_providers}

        mock_get_object.side_effect = self.test_providers

        returned_page_objects = cqc.get_page_objects(
            self.test_url, 1, self.provider_object_type, CQCP.provider_id, "PARTNERCODE"
        )

        expected_page_objects = self.test_providers
        self.assertEqual(returned_page_objects, expected_page_objects)

        mock_call_api.assert_called_once_with(
            self.test_url,
            {"page": 1, "perPage": 500, "partnerCode": "PARTNERCODE"},
            headers_dict={"User-Agent": "SkillsForCare"},
        )

        mock_get_object.assert_has_calls(
            [
                call(self.provider_id_examples[0], self.provider_object_type),
                call(self.provider_id_examples[1], self.provider_object_type),
                call(self.provider_id_examples[2], self.provider_object_type),
            ]
        )


if __name__ == "__main__":
    unittest.main()
