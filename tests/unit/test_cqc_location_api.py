from utils import cqc_api as cqc
from pyspark.sql import SparkSession
import mock
import unittest


class TestCQCLocationAPI(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @mock.patch('utils.cqc_api.call_api')
    def test_get_location(self, mock_call_api):
        mock_call_api.return_value = {"locationId": "test_id"}

        location_body = {
            "locationId": "test_id"
        }

        result = cqc.get_location("test_id")
        self.assertEqual(result, location_body)

    @mock.patch('utils.cqc_api.call_api')
    @mock.patch('utils.cqc_api.get_location')
    def test_get_page_locations(self, mock_get_location, mock_call_api):
        mock_call_api.return_value = {"locations": [
            {"locationId": "test_id"},
            {"locationId": "test_id_2"},
            {"locationId": "test_id_3"},
        ]}

        mock_get_location.return_value(
            {"locationId": "get_location_return_id"})

        result = cqc.get_page_objects(
            "test_url", 1, "locations", "locationId")

        mock_call_api.assert_called_once_with(
            "test_url", {'page': 1, 'perPage': 500})

        mock_get_location.assert_has_calls(
            [mock.call("test_id"), mock.call("test_id_2"), mock.call("test_id_3")])


if __name__ == '__main__':
    unittest.main()
