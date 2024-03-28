import unittest
import re

from utils import cqc_api_new as cqc

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)

EXAMPLE_LOCATION_ID = "1-10000792582"
LOCATION_ID_REGEX = r"[0-9]-[0-9]{11}"

# TODO : test call_api()
# TODO : test get_all_objects()


class CqcApiIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.PARTNER_CODE_STUB = "PARTNERCODE"
        self.page = 1


class LocationApiTests(CqcApiIntegrationTests):
    def setUp(self) -> None:
        super().setUp()
        self.object_type = "locations"
        self.example_object = [
            {CQCL.location_id: "1-10000792582"},
            {CQCL.provider_id: "1-9098203603"},
            {CQCL.organisation_type: "Location"},
        ]

    def test_get_object_returns_location(self):
        result = cqc.get_object(EXAMPLE_LOCATION_ID, self.object_type)

        self.assertEqual(
            result[CQCL.location_id], self.example_object[CQCL.location_id]
        )
        self.assertEqual(
            result[CQCL.provider_id], self.example_object[CQCL.provider_id]
        )
        self.assertEqual(
            result[CQCL.organisation_type], self.example_object[CQCL.organisation_type]
        )

    def test_get_page_locations_returns_all_locations_for_page(self):
        url = f"{cqc.CQC_API_BASE_URL}/public/{cqc.CQC_API_VERSION}/{self.object_type}"

        locations = cqc.get_page_objects(
            url,
            self.page,
            self.object_type,
            CQCL.location_id,
            self.PARTNER_CODE_STUB,
            per_page=5,
        )
        self.assertEqual(len(locations), 5)

        regex = re.compile(LOCATION_ID_REGEX)
        for location in locations:
            self.assertTrue(regex.match(location[CQCL.location_id]))
            self.assertIsNotNone(location[CQCL.provider_id])


class ProviderApiTests(CqcApiIntegrationTests):
    def setUp(self) -> None:
        super().setUp()
        self.object_type = "providers"
        self.example_object = [
            {CQCP.location_ids: ["1-10000792582"]},
            {CQCP.provider_id: "1-9098203603"},
            {CQCP.organisation_type: "Provider"},
        ]

    def test_get_object_returns_provider(self):
        result = cqc.get_object(EXAMPLE_LOCATION_ID, self.object_type)

        self.assertEqual(
            result[CQCP.location_ids], self.example_object[CQCP.location_ids]
        )
        self.assertEqual(
            result[CQCP.provider_id], self.example_object[CQCP.provider_id]
        )
        self.assertEqual(
            result[CQCP.organisation_type], self.example_object[CQCP.organisation_type]
        )

    def test_get_page_providers_returns_all_providers_for_page(self):
        url = f"{cqc.CQC_API_BASE_URL}/public/{cqc.CQC_API_VERSION}/{self.object_type}"

        providers = cqc.get_page_objects(
            url,
            self.page,
            self.object_type,
            CQCP.provider_id,
            self.PARTNER_CODE_STUB,
            per_page=5,
        )
        self.assertEqual(len(providers), 5)

        regex = re.compile(LOCATION_ID_REGEX)
        for provider in providers:
            self.assertTrue(regex.match(provider[CQCP.provider_id]))
            self.assertIsNotNone(provider[CQCP.location_ids])


if __name__ == "__main__":
    unittest.main()
