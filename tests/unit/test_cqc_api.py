import unittest
from unittest.mock import Mock, patch, call
from typing import Generator

from utils import cqc_api as cqc


class CqcApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_url = "test_url"
        self.location_object_type = "locations"
        self.test_column = "test_column"
        self.value_examples = ["value_1", "value_2", "value_3"]
        self.test_data = [
            {self.test_column: self.value_examples[0]},
            {self.test_column: self.value_examples[1]},
            {self.test_column: self.value_examples[2]},
        ]
        self.cqc_api_primary_key_stub = "cqc_api_primary_key"


class TestResponse:
    status_code: int = 500
    content: dict = {}

    def __init__(self, status_code: int, content: dict) -> None:
        self.status_code = status_code
        self.content = content

    def json(self):
        return self.content


class GetObjectTests(CqcApiTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.cqc_api.call_api")
    def test_get_object_when_getting_location(self, mock_call_api: Mock):
        mock_call_api.return_value = self.test_data[0]

        returned_location = cqc.get_object(
            self.test_data[0][self.test_column],
            self.location_object_type,
            self.cqc_api_primary_key_stub,
        )
        expected_location = self.test_data[0]

        self.assertEqual(returned_location, expected_location)


class GetPageObjectsTests(CqcApiTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.cqc_api.call_api")
    @patch("utils.cqc_api.get_object")
    def test_get_page_objects_when_getting_locations(
        self, mock_get_object: Mock, mock_call_api: Mock
    ):
        mock_call_api.return_value = {self.location_object_type: self.test_data}

        mock_get_object.side_effect = self.test_data

        returned_page_objects = cqc.get_page_objects(
            self.test_url,
            1,
            self.location_object_type,
            self.test_column,
            self.cqc_api_primary_key_stub,
        )

        expected_page_objects = self.test_data
        self.assertEqual(returned_page_objects, expected_page_objects)

        mock_call_api.assert_called_once_with(
            self.test_url,
            {"page": 1, "perPage": 500},
            headers_dict={
                "User-Agent": "SkillsForCare",
                "Ocp-Apim-Subscription-Key": self.cqc_api_primary_key_stub,
            },
        )

        mock_get_object.assert_has_calls(
            [
                call(
                    self.value_examples[0],
                    self.location_object_type,
                    self.cqc_api_primary_key_stub,
                ),
                call(
                    self.value_examples[1],
                    self.location_object_type,
                    self.cqc_api_primary_key_stub,
                ),
                call(
                    self.value_examples[2],
                    self.location_object_type,
                    self.cqc_api_primary_key_stub,
                ),
            ]
        )


class CallApiTests(CqcApiTests):
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


class GetAllObjectsTests(CqcApiTests):
    @unittest.skip("Skip whilst testing step function")
    @patch("utils.cqc_api.get_page_objects")
    @patch("utils.cqc_api.call_api")
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
            cqc_api_primary_key="cqc_api_primary_key",
        )

        self.assertTrue(isinstance(generator, Generator))
        self.assertEqual(next(generator), test_response_page_1_json)
        self.assertEqual(next(generator), test_response_page_2_json)


if __name__ == "__main__":
    unittest.main()
