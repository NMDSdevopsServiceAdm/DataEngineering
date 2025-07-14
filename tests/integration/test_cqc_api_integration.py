from typing import Generator
import unittest
import re
import json

from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from utils import (
    aws_secrets_manager_utilities as ars,
)

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)


LOCATION_ID_REGEX = r"[0-9]-[0-9]{11}"


class CqcApiIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.cqc_api_primary_key = json.loads(
           ars.get_secret(secret_name="cqc_api_primary_key", region_name="eu-west-2")
        )["Ocp-Apim-Subscription-Key"]
        self.page = 1


class LocationApiTests(CqcApiIntegrationTests):
    def setUp(self) -> None:
        super().setUp()
        self.object_type = "locations"
        self.organisation_type = "location"
        self.example_object = {
            CQCL.location_id: "1-10000792582",
            CQCL.provider_id: "1-9098203603",
            CQCL.organisation_type: "Location",
        }

    def test_get_object_returns_location(self):
        result = cqc.get_object(
            self.example_object[CQCL.location_id],
            self.object_type,
            self.cqc_api_primary_key,
        )
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
            self.cqc_api_primary_key,
            per_page=5,
        )
        self.assertEqual(len(locations), 5)

        regex = re.compile(LOCATION_ID_REGEX)
        for location in locations:
            self.assertTrue(regex.match(location[CQCL.location_id]))
            self.assertIsNotNone(location[CQCL.provider_id])

    def test_get_updated_objects(self):
        with self.subTest("small-window"):
            # Given
            known_changes_size = 4
            start_time = "2025-06-01T00:00:00Z"
            end_time = "2025-06-01T16:00:00Z"

            # When
            result = cqc.get_updated_objects(
                self.object_type,
                self.organisation_type,
                self.cqc_api_primary_key,
                start_time,
                end_time,
                per_page=10
            )
            # Then
            self.assertTrue(isinstance(result, Generator))
            for idx, change in enumerate(result, start=1):
                self.assertTrue(set(change.keys()).issuperset(self.example_object.keys()))
                if result.__next__() is None:
                    self.assertEqual(idx, known_changes_size)

        with self.subTest("zero-window"):
            # Given
            same_time = "2025-06-01T00:00:00Z"

            # When
            result = cqc.get_updated_objects(
                self.object_type,
                self.organisation_type,
                self.cqc_api_primary_key,
                same_time,
                same_time,
                per_page=10
            )
            # Then
            self.assertIsNone(next(result, None))

class ProviderApiTests(CqcApiIntegrationTests):
    def setUp(self) -> None:
        super().setUp()
        self.object_type = "providers"
        self.organisation_type = "provider"
        self.example_object = {
            CQCP.location_ids: ["1-10000792582"],
            CQCP.provider_id: "1-9098203603",
            CQCP.organisation_type: "Provider",
        }

    def test_get_object_returns_provider(self):
        result = cqc.get_object(
            self.example_object[CQCP.provider_id],
            self.object_type,
            self.cqc_api_primary_key,
        )

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
            self.cqc_api_primary_key,
            per_page=5,
        )
        self.assertEqual(len(providers), 5)

        regex = re.compile(LOCATION_ID_REGEX)
        for provider in providers:
            self.assertTrue(regex.match(provider[CQCP.provider_id]))
            self.assertIsNotNone(provider[CQCP.location_ids])


if __name__ == "__main__":
    unittest.main()
