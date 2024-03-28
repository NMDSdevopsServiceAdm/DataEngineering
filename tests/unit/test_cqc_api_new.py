import unittest
from unittest.mock import Mock, patch, call
from typing import Generator

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


class ResponseTests:
    @patch("requests.get")
    def test_call_api_handles_200(self, get_mock: Mock):
        test_response = TestResponse(200, {})
        get_mock.return_value = test_response

        response_json = cqc.call_api(
            "test_url", {"test": "body"}, headers_dict={"some": "header"}
        )
        self.assertEqual(response_json, {})

    @patch("requests.get")
    def test_call_api_handles_500(self, get_mock: Mock):
        test_response = TestResponse(500, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict={"some": "header"})

        self.assertTrue("API response: 500" in str(context.exception))

    @patch("requests.get")
    def test_call_api_handles_400(self, get_mock: Mock):
        test_response = TestResponse(400, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict={"some": "header"})

        self.assertTrue("API response: 400" in str(context.exception))

    @patch("requests.get")
    def test_call_api_handles_404(self, get_mock: Mock):
        test_response = TestResponse(404, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict={"some": "header"})

        self.assertTrue("API response: 404" in str(context.exception))

    @patch("requests.get")
    def test_call_api_handles_403_with_headers(self, get_mock: Mock):
        test_response = TestResponse(403, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict={"some": "header"})

        self.assertTrue("API response: 403" in str(context.exception))

    @patch("requests.get")
    def test_call_api_handles_403_without_headers(self, get_mock: Mock):
        test_response = TestResponse(403, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict=None)

        self.assertTrue(
            "API response: 403, ensure you have set a User-Agent header"
            in str(context.exception)
        )

    @patch("time.sleep", return_value=None)
    @patch("requests.get")
    def test_call_api_handles_429_without_headers(
        self, get_mock: Mock, sleep_mock: Mock
    ):
        test_response_timeout = TestResponse(429, {})
        test_response_success = TestResponse(200, {})
        get_mock.side_effect = [test_response_timeout, test_response_success]

        response_json = cqc.call_api(
            "test_url", {"test": "body"}, headers_dict={"some": "header"}
        )

        sleep_mock.assert_called_once()
        self.assertEqual(response_json, {})

    @patch("utils.cqc_api_new.get_page_objects")
    @patch("utils.cqc_api_new.call_api")
    def test_get_all_objects_returns_correct_generator(
        self, call_api_mock: Mock, get_page_objects_mock: Mock
    ):
        test_response_page_1_json = {"locations": ["1"]}
        test_response_page_2_json = {"locations": ["2"]}
        call_api_mock.return_value = {"totalPages": 2}
        get_page_objects_mock.side_effect = [
            test_response_page_1_json,
            test_response_page_2_json,
        ]

        generator = cqc.get_all_objects(
            object_type="locations",
            object_identifier="location_id",
            partner_code="partner_code",
        )

        self.assertTrue(isinstance(generator, Generator))
        self.assertEqual(next(generator), test_response_page_1_json)
        self.assertEqual(next(generator), test_response_page_2_json)


if __name__ == "__main__":
    unittest.main()
