from utils import cqc_api_new as cqc
import unittest
import re

EXAMPLE_LOCATION_ID = "1-10000792582"
LOCATION_ID_REGEX = r"[0-9]-[0-9]{11}"


class TestCQCLocationAPIIntegration(unittest.TestCase):
    def setUp(self):
        self.PARTNER_CODE_STUB = "PARTNERCODE"

    def test_get_object_returns_object(self):
        result = cqc.get_object(EXAMPLE_LOCATION_ID, "locations")

        self.assertEqual(result["locationId"], EXAMPLE_LOCATION_ID)
        self.assertEqual(result["providerId"], "1-9098203603")
        self.assertEqual(result["organisationType"], "Location")

    def test_get_page_objects_returns_all_objects_for_page(self):
        page = 1
        url = f"https://api.cqc.org.uk/public/v1/locations"

        locations = cqc.get_page_objects(
            url, page, "locations", "locationId", self.PARTNER_CODE_STUB, per_page=5
        )
        self.assertEqual(len(locations), 5)

        regex = re.compile(LOCATION_ID_REGEX)
        for location in locations:
            self.assertTrue(regex.match(location["locationId"]))
            self.assertIsNotNone(location["providerId"])


if __name__ == "__main__":
    unittest.main()
