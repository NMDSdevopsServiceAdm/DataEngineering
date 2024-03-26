import unittest

import mock

from utils import cqc_api_new as cqc


class TestResponse:
    status_code: int = 500
    content: dict = {}

    def __init__(self, status_code: int, content: dict) -> None:
        self.status_code = status_code
        self.content = content

    def json(self):
        return self.content


class TestCQCLocationAPI(unittest.TestCase):
    @mock.patch("utils.cqc_api_new.call_api")
    def test_get_location(self, mock_call_api):
        mock_call_api.return_value = {"locationId": "test_id"}

        location_body = {"locationId": "test_id"}

        result = cqc.get_object("test_id", "locations")
        self.assertEqual(result, location_body)

    @mock.patch("utils.cqc_api_new.call_api")
    @mock.patch("utils.cqc_api_new.get_object")
    def test_get_page_locations(self, mock_get_object, mock_call_api):
        mock_call_api.return_value = {
            "locations": [
                {"locationId": "test_id"},
                {"locationId": "test_id_2"},
                {"locationId": "test_id_3"},
            ]
        }

        mock_get_object.return_value({"locationId": "get_location_return_id"})

        cqc.get_page_objects("test_url", 1, "locations", "locationId", "PARTNERCODE")

        mock_call_api.assert_called_once_with(
            "test_url",
            {"page": 1, "perPage": 500, "partnerCode": "PARTNERCODE"},
            headers_dict={"User-Agent": "SkillsForCare"},
        )

        mock_get_object.assert_has_calls(
            [
                mock.call("test_id", "locations"),
                mock.call("test_id_2", "locations"),
                mock.call("test_id_3", "locations"),
            ]
        )

    @mock.patch("requests.get")
    def test_call_api_handles_200(self, get_mock: mock.Mock):
        test_response = TestResponse(200, {})
        get_mock.return_value = test_response

        response_json = cqc.call_api(
            "test_url", {"test": "body"}, headers_dict={"some": "header"}
        )
        self.assertEqual(response_json, {})

    @mock.patch("requests.get")
    def test_call_api_handles_500(self, get_mock: mock.Mock):
        test_response = TestResponse(500, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict={"some": "header"})

        self.assertTrue("API response: 500" in str(context.exception))

    @mock.patch("requests.get")
    def test_call_api_handles_403_with_headers(self, get_mock: mock.Mock):
        test_response = TestResponse(403, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict={"some": "header"})

        self.assertTrue("API response: 403" in str(context.exception))

    @mock.patch("requests.get")
    def test_call_api_handles_403_without_headers(self, get_mock: mock.Mock):
        test_response = TestResponse(403, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api("test_url", {"test": "body"}, headers_dict=None)

        self.assertTrue(
            "API response: 403, ensure you have set a User-Agent header"
            in str(context.exception)
        )

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("requests.get")
    def test_call_api_handles_429_without_headers(
        self, get_mock: mock.Mock, sleep_mock: mock.Mock
    ):
        test_response_timeout = TestResponse(429, {})
        test_response_success = TestResponse(200, {})
        get_mock.side_effect = [test_response_timeout, test_response_success]

        response_json = cqc.call_api(
            "test_url", {"test": "body"}, headers_dict={"some": "header"}
        )

        sleep_mock.assert_called_once()
        self.assertEqual(response_json, {})


if __name__ == "__main__":
    unittest.main()
