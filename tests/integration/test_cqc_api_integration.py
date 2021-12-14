from utils import cqc_location_api as cqc
from pyspark.sql import SparkSession
import mock
import unittest
import requests
import re

EXAMPLE_LOCATION_ID = "1-10000792582"
LOCATION_ID_REGEX = r"[0-9]-[0-9]{11}"


class TestCQCLocationAPIIntegration(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_location_returns_location(self):

        result = cqc.get_location(EXAMPLE_LOCATION_ID)

        self.assertEqual(result["locationId"], EXAMPLE_LOCATION_ID)
        self.assertEqual(result["providerId"], "1-9098203603")
        self.assertEqual(result["organisationType"], "Location")

    def test_get_page_locations_returns_all_locations_for_page(self):

        page_size = 10
        page = 1
        url = f"https://api.cqc.org.uk/public/v1/locations"

        locations = cqc.get_page_locations(url, page, 5)
        self.assertEqual(len(locations), 5)

        regex = re.compile(LOCATION_ID_REGEX)
        for location in locations:
            self.assertTrue(regex.match(location['locationId']))
            self.assertIsNotNone(location["providerId"])


if __name__ == '__main__':
    unittest.main()
