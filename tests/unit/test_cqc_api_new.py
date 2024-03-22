import mock
import unittest

from utils import cqc_api_new as cqc


class TestCQCLocationAPI(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

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

        result = cqc.get_page_objects(
            "test_url", 1, "locations", "locationId", "PARTNERCODE"
        )

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


if __name__ == "__main__":
    unittest.main()
