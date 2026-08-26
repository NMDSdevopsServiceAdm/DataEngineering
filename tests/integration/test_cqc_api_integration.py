import json
import re
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator

import pytest
import requests

from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from utils import aws_secrets_manager_utilities as ars
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)

LOCATION_ID_REGEX = r"[0-9]-[0-9]{11}"

CQC_OUTAGE_STATUS_CODES = {500, 502, 503, 504}
CQC_GATEWAY_SIGNATURE = "Microsoft-Azure-Application-Gateway"


def _is_cqc_api_outage(exc: Exception) -> bool:
    if isinstance(
        exc, (requests.exceptions.RequestException, cqc.CqcApiRateLimitedException)
    ):
        return True
    message = str(exc)
    if message.startswith("Max retries exceeded"):
        return True
    match = re.match(r"API response: (\d+)", message)
    if match:
        status_code = int(match.group(1))
        if status_code in CQC_OUTAGE_STATUS_CODES:
            return True
        if status_code == 403 and CQC_GATEWAY_SIGNATURE in message:
            return True
    return False


@dataclass
class CqcApiOutageTestCase:
    id: str
    exception: Exception
    expected_is_outage: bool

    def as_pytest_param(self):
        return pytest.param(self.exception, self.expected_is_outage, id=self.id)


cqc_api_outage_test_cases = [
    CqcApiOutageTestCase(
        id="gateway_403_html_page_is_outage",
        exception=Exception(
            "API response: 403 - <html><hr><center>"
            "Microsoft-Azure-Application-Gateway/v2</center></html>"
        ),
        expected_is_outage=True,
    ),
    CqcApiOutageTestCase(
        id="server_5xx_is_outage",
        exception=Exception("API response: 503 - Service Unavailable"),
        expected_is_outage=True,
    ),
    CqcApiOutageTestCase(
        id="retry_exhaustion_is_outage",
        exception=Exception("Max retries exceeded: some detail"),
        expected_is_outage=True,
    ),
    CqcApiOutageTestCase(
        id="raw_connection_error_is_outage",
        exception=requests.exceptions.ConnectionError("boom"),
        expected_is_outage=True,
    ),
    CqcApiOutageTestCase(
        id="raw_timeout_is_outage",
        exception=requests.exceptions.Timeout("boom"),
        expected_is_outage=True,
    ),
    CqcApiOutageTestCase(
        id="exhausted_soft_rate_limit_is_outage",
        exception=cqc.CqcApiRateLimitedException(
            "CQC API soft rate limit not resolved after 5 retries: "
            "{'statusCode': 429, 'message': 'Rate limit is exceeded.'}"
        ),
        expected_is_outage=True,
    ),
    CqcApiOutageTestCase(
        id="missing_user_agent_403_is_not_outage",
        exception=Exception(
            "API response: 403, ensure you have set a User-Agent header"
        ),
        expected_is_outage=False,
    ),
    CqcApiOutageTestCase(
        id="not_found_404_is_not_outage",
        exception=cqc.NoProviderOrLocationException("API response: 404 - not found"),
        expected_is_outage=False,
    ),
    CqcApiOutageTestCase(
        id="assertion_failure_is_not_outage",
        exception=AssertionError("1 != 2"),
        expected_is_outage=False,
    ),
]


class TestIsCqcApiOutage:
    @pytest.mark.parametrize(
        "exception, expected_is_outage",
        [c.as_pytest_param() for c in cqc_api_outage_test_cases],
    )
    def test_returns_expected_result_for_exception_shape(
        self, exception, expected_is_outage
    ):
        assert _is_cqc_api_outage(exception) == expected_is_outage


class RedactedSecret(str):
    """String subclass whose repr is redacted.

    Behaves as a normal string everywhere (HTTP headers, formatting), but
    pytest's default traceback prints each frame's locals via repr() -- this
    stops a real API key from ending up in CI logs when a call using it fails.
    """

    def __repr__(self) -> str:
        return "'<redacted>'"


class CqcApiIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.cqc_api_primary_key = RedactedSecret(
            json.loads(
                ars.get_secret(
                    secret_name="cqc_api_primary_key", region_name="eu-west-2"
                )
            )["Ocp-Apim-Subscription-Key"]
        )
        self.page = 1

    @contextmanager
    def skip_on_cqc_outage(self):
        try:
            yield
        except Exception as exc:
            if _is_cqc_api_outage(exc):
                pytest.skip(f"CQC API unavailable: {exc}")
            raise


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
        with self.skip_on_cqc_outage():
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
                result[CQCL.organisation_type],
                self.example_object[CQCL.organisation_type],
            )

    def test_get_page_locations_returns_all_locations_for_page(self):
        url = f"{cqc.CQC_API_BASE_URL}/public/{cqc.CQC_API_VERSION}/{self.object_type}"

        with self.skip_on_cqc_outage():
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

    def test_get_updated_objects_known_quantity(self):
        # Given
        known_changes_size = 4  # manually verified number of changes for timeframe
        start_time = "2025-06-01T00:00:00Z"
        end_time = "2025-06-01T16:00:00Z"

        with self.skip_on_cqc_outage():
            # When
            result = cqc.get_updated_objects(
                self.object_type,
                self.organisation_type,
                self.cqc_api_primary_key,
                start_time,
                end_time,
                per_page=10,
            )
            # Then
            self.assertTrue(isinstance(result, Generator))
            for idx, change in enumerate(result, start=1):
                self.assertTrue(
                    set(change.keys()).issuperset(self.example_object.keys())
                )
                if result.__next__() is None:
                    self.assertEqual(idx, known_changes_size)

    def test_get_updated_objects_zero_time(self):
        # Given
        same_time = "2025-06-01T00:00:00Z"

        with self.skip_on_cqc_outage():
            # When
            result = cqc.get_updated_objects(
                self.object_type,
                self.organisation_type,
                self.cqc_api_primary_key,
                same_time,
                same_time,
                per_page=10,
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
        with self.skip_on_cqc_outage():
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
                result[CQCP.organisation_type],
                self.example_object[CQCP.organisation_type],
            )

    def test_get_page_providers_returns_all_providers_for_page(self):
        url = f"{cqc.CQC_API_BASE_URL}/public/{cqc.CQC_API_VERSION}/{self.object_type}"

        with self.skip_on_cqc_outage():
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
