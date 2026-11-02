import io
import unittest
from contextlib import redirect_stdout
from dataclasses import dataclass
from http.client import HTTPMessage
from typing import Generator
from unittest.mock import Mock, call, patch

import polars as pl
import polars.testing as pl_testing
import pytest
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from urllib3.util.retry import RequestHistory, Retry

from projects._01_ingest.cqc_api.utils import cqc_api as cqc

PATCH_PATH = "projects._01_ingest.cqc_api.utils.cqc_api"

PATCHED_CQC_ADAPTER = HTTPAdapter(
    max_retries=Retry(
        total=3,
        backoff_factor=0.1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
)


class CqcApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_url = "test_url"
        self.test_location_id = "1-001"
        self.test_lc_location_id = "lower"
        self.location_object_type = "locations"
        self.test_column = "test_column"
        self.value_examples = ["value_1", "value_2", "value_3"]
        self.test_data = [
            {self.test_column: self.value_examples[0]},
            {self.test_column: self.value_examples[1]},
            {self.test_column: self.value_examples[2]},
        ]
        self.cqc_api_primary_key_stub = "cqc_api_primary_key"

    def gen(self, *rows: dict) -> Generator[dict, None, None]:
        yield from rows


class TestResponse:
    # Adding so pytest doesn't try to collect as a Test Classß.
    __test__ = False
    status_code: int = 500
    content: dict = {}
    text: str = ""

    def __init__(self, status_code: int, content: dict, text: str = "") -> None:
        self.status_code = status_code
        self.content = content

    def json(self):
        return self.content


class GetObjectTests(CqcApiTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.call_api")
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

    @patch(f"{PATCH_PATH}.call_api")
    @patch(f"{PATCH_PATH}.get_object")
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
            query_params={"page": 1, "perPage": 500},
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


CALL_API_ARGS = {
    "url": "test_url",
    "id": "1-001",
    "query_params": {"test": "body"},
    "headers_dict": {"some": "header"},
}
RETRY_URL = "https://api.service.cqc.org.uk/test"


@dataclass
class StatusCodeTestCase:
    id: str
    status_code: int

    def as_pytest_param(self):
        return pytest.param(self.status_code, id=self.id)


unexpected_status_cases = [
    StatusCodeTestCase(id="server_error_500", status_code=500),
    StatusCodeTestCase(id="forbidden_403", status_code=403),
    StatusCodeTestCase(id="bad_request_400_when_id_is_uppercase", status_code=400),
]

retryable_status_cases = [
    StatusCodeTestCase(id=f"status_{code}", status_code=code)
    for code in [429, 500, 502, 503, 504]
]


def mock_connection_responses(
    getconn_mock: Mock, response_mock: Mock, statuses: list[int]
) -> None:
    """Returns the given statuses from the mocked connection, in order.

    A 200 is built into a response whose JSON is `{"data": "success"}`.
    """
    conn_responses = []
    for status in statuses:
        conn_response = Mock(name=f"conn_response_{status}")
        conn_response.status = status
        conn_response.headers = {}
        conn_response.msg = HTTPMessage()
        conn_responses.append(conn_response)
    getconn_mock.side_effect = conn_responses

    response_mock.return_value = Mock(name="mock_response_200", history=False)
    response_mock.return_value.status_code = 200
    response_mock.return_value.json.return_value = {"data": "success"}


class TestCallApi:
    @patch("requests.Session.get")
    def test_returns_json_on_200(self, get_mock: Mock):
        get_mock.return_value = TestResponse(200, {"data": "success"})

        assert cqc.call_api(**CALL_API_ARGS) == {"data": "success"}

    @pytest.mark.parametrize(
        "status_code", [c.as_pytest_param() for c in unexpected_status_cases]
    )
    @patch("requests.Session.get")
    def test_raises_api_response_error_on_unexpected_status(
        self, get_mock: Mock, status_code: int
    ):
        get_mock.return_value = TestResponse(status_code, {})

        with pytest.raises(Exception, match=f"^API response: {status_code} -"):
            cqc.call_api(**CALL_API_ARGS)

    @patch("requests.Session.get")
    def test_raises_user_agent_hint_on_403_without_headers(self, get_mock: Mock):
        get_mock.return_value = TestResponse(403, {})

        with pytest.raises(
            Exception,
            match="API response: 403, ensure you have set a User-Agent header",
        ):
            cqc.call_api(**{**CALL_API_ARGS, "headers_dict": None})

    @patch("requests.Session.get")
    def test_raises_no_provider_or_location_on_404(self, get_mock: Mock):
        get_mock.return_value = TestResponse(404, {})

        with pytest.raises(
            cqc.NoProviderOrLocationException, match="^API response: 404 -"
        ):
            cqc.call_api(**CALL_API_ARGS)

    @patch("requests.Session.get")
    def test_retries_with_uppercase_id_on_400_lowercase_id(self, get_mock: Mock):
        get_mock.side_effect = [TestResponse(400, {}), TestResponse(200, {})]

        response_json = cqc.call_api(**{**CALL_API_ARGS, "id": "lower"})

        get_mock.assert_has_calls(
            [
                call(
                    "test_urllower", params={"test": "body"}, headers={"some": "header"}
                ),
                call(
                    "test_urlLOWER", params={"test": "body"}, headers={"some": "header"}
                ),
            ]
        )
        assert response_json == {}

    @pytest.mark.parametrize(
        "status_code", [c.as_pytest_param() for c in retryable_status_cases]
    )
    @patch(f"{PATCH_PATH}.CQC_ADAPTER", PATCHED_CQC_ADAPTER)
    @patch("requests.sessions.SessionRedirectMixin.resolve_redirects")
    @patch("requests.adapters.HTTPAdapter.build_response")
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_retries_on_retryable_status(
        self, getconn_mock: Mock, response_mock: Mock, _, status_code: int
    ):
        mock_connection_responses(
            getconn_mock, response_mock, [status_code, status_code, 200]
        )

        result = cqc.call_api(
            url=RETRY_URL, id="1-001", query_params={"param": "value"}
        )

        assert result == {"data": "success"}
        assert getconn_mock.call_count == 3

    @patch(f"{PATCH_PATH}.CQC_ADAPTER", PATCHED_CQC_ADAPTER)
    @patch("requests.sessions.SessionRedirectMixin.resolve_redirects")
    @patch("requests.adapters.HTTPAdapter.build_response")
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_raises_retries_exhausted_after_max_retries(
        self, getconn_mock: Mock, response_mock: Mock, _
    ):
        mock_connection_responses(getconn_mock, response_mock, [502] * 4 + [200])

        with pytest.raises(Exception, match="^CQC API retries exhausted: "):
            cqc.call_api(url=RETRY_URL, id="1-001", query_params={"param": "value"})

        assert getconn_mock.call_count == 4  # total=3 allows 3 retries after the first

    @patch("requests.Session.get")
    def test_raises_retries_exhausted_on_retry_error(self, get_mock: Mock):
        get_mock.side_effect = RetryError("too many 502 error responses")

        with pytest.raises(
            Exception,
            match="^CQC API retries exhausted: .*too many 502 error responses",
        ):
            cqc.call_api(**CALL_API_ARGS)

    @patch("requests.Session.get")
    def test_raises_retries_exhausted_on_retry_error_when_retrying_uppercase_id(
        self, get_mock: Mock
    ):
        get_mock.side_effect = [
            TestResponse(400, {}),
            RetryError("too many 502 error responses"),
        ]

        with pytest.raises(
            Exception,
            match="^CQC API retries exhausted: .*too many 502 error responses",
        ):
            cqc.call_api(**{**CALL_API_ARGS, "id": "lower"})


class GetUpdatedObjectsTests(CqcApiTests):
    @patch(f"{PATCH_PATH}.get_changes_within_timeframe")
    @patch(f"{PATCH_PATH}.get_object")
    def test_get_updated_objects(self, mock_get_object: Mock, mock_get_changes: Mock):
        # Given
        changed_ids = ["1", "2", "5", "6", "9"]
        expected_changes = [{"id": id} for id in changed_ids]

        mock_get_changes.side_effect = [
            {"page": 1, "totalPages": 3, "changes": ["1", "2"]},
            {"page": 2, "totalPages": 3, "changes": ["5", "6"]},
            {"page": 3, "totalPages": 3, "changes": ["9"]},
        ]
        mock_get_object.side_effect = expected_changes
        # When
        results = cqc.get_updated_objects(
            object_type="any",
            organisation_type="any_org_type",
            cqc_api_primary_key="cqc_api_primary_key",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-02T00:00:00Z",
        )
        # Then
        self.assertTrue(isinstance(results, Generator))
        for expected_change in expected_changes:
            self.assertEqual(next(results), expected_change)
        mock_get_object.assert_has_calls(
            [call(id, "any", "cqc_api_primary_key") for id in changed_ids],
            any_order=True,
        )


class GetChangesWithinTimeframeTests(CqcApiTests):
    @patch(f"{PATCH_PATH}.call_api")
    def test_get_changes_within_timeframe(self, mock_call_api: Mock):
        # Given
        mock_call_api.return_value = {"changes": ["1", "2", "3"]}
        # When
        result = cqc.get_changes_within_timeframe(
            organisation_type="mock_organisations",
            cqc_api_primary_key="cqc_api_primary_key",
            start_timestamp="2000-01-01T00:00:00Z",
            end_timestamp="2000-01-02T00:00:00Z",
            page_number=5,
            per_page=100,
        )
        # Then
        mock_call_api.assert_called_once_with(
            "https://api.service.cqc.org.uk/public/v1/changes/mock_organisations",
            query_params={
                "startTimestamp": "2000-01-01T00:00:00Z",
                "endTimestamp": "2000-01-02T00:00:00Z",
                "page": 5,
                "perPage": 100,
            },
            headers_dict={
                "User-Agent": "SkillsForCare",
                "Ocp-Apim-Subscription-Key": "cqc_api_primary_key",
            },
        )
        self.assertEqual(result, {"changes": ["1", "2", "3"]})


class NormaliseStructsTests(CqcApiTests):
    def test_adds_missing_struct_fields(self):
        schema = {
            "address": pl.Struct(
                [
                    pl.Field("line1", pl.Utf8),
                    pl.Field("postcode", pl.Utf8),
                ]
            )
        }

        record = {"address": {"line1": "123 Main St"}}
        expected = {"address": {"line1": "123 Main St", "postcode": None}}

        returned = cqc.normalise_structs(record, schema)
        self.assertEqual(returned, expected)

    def test_struct_invalid_or_missing_value_becomes_null_struct(self):
        schema = {
            "address": pl.Struct(
                [pl.Field("line1", pl.Utf8), pl.Field("postcode", pl.Utf8)]
            )
        }
        scenarios = [
            {"address": None},  # null value
            {"address": "bad_value"},  # non-dict value
            {},  # key missing entirely
        ]
        for record in scenarios:
            with self.subTest(record=record):
                returned = cqc.normalise_structs(record, schema)
                self.assertEqual(returned["address"], {"line1": None, "postcode": None})

    def test_strips_extra_fields_not_in_schema(self):
        schema = {
            "address": pl.Struct(
                [
                    pl.Field("line1", pl.Utf8),
                    pl.Field("postcode", pl.Utf8),
                ]
            )
        }

        record = {
            "address": {
                "line1": "123 Main St",
                "postcode": "AB1 2CD",
                "extra": "IGNORE",
            }
        }

        expected = {"address": {"line1": "123 Main St", "postcode": "AB1 2CD"}}

        returned = cqc.normalise_structs(record, schema)
        self.assertEqual(returned, expected)

    def test_normalise_structs_keeps_column_not_in_schema(self):
        schema = {
            "address": pl.Struct(
                [
                    pl.Field("line1", pl.Utf8),
                    pl.Field("postcode", pl.Utf8),
                ]
            )
        }

        record = {
            "name": "Care Home",
            "address": {"line1": "123 Main St", "postcode": "AB1 2CD"},
        }

        returned = cqc.normalise_structs(record, schema)
        self.assertEqual(returned, record)

    def test_normalise_structs_does_not_change_original_data_type_to_match_schema(self):
        schema = {
            "struct_col": pl.Struct(
                [
                    pl.Field("number", pl.Int32),
                    pl.Field("string_date", pl.Utf8),
                ]
            ),
        }

        record = {
            "struct_col": {"number": "123", "string_date": "2025-01-01"},
        }

        returned = cqc.normalise_structs(record, schema)
        self.assertEqual(returned, record)

    def test_list_of_structs_normalisation(self):
        schema = {
            "contacts": pl.List(
                pl.Struct([pl.Field("name", pl.Utf8), pl.Field("phone", pl.Utf8)])
            )
        }
        scenarios = [
            (
                {"contacts": [{"name": "Alice"}]},
                [{"name": "Alice", "phone": None}],
            ),  # missing inner field → None
            ({"contacts": None}, []),  # null list → []
            (
                {"contacts": ["not_a_dict"]},
                [{"name": None, "phone": None}],
            ),  # non-dict item → null struct
        ]
        for record, expected in scenarios:
            with self.subTest(record=record):
                returned = cqc.normalise_structs(record, schema)
                self.assertEqual(returned["contacts"], expected)

    def test_list_scalar_normalisation(self):
        schema = {"tags": pl.List(pl.Utf8)}
        self.assertEqual(
            cqc.normalise_structs({"tags": ["a", "b"]}, schema)["tags"], ["a", "b"]
        )
        self.assertEqual(cqc.normalise_structs({"tags": None}, schema)["tags"], [])


class BuildDataframeFromApi(CqcApiTests):
    def setUp(self) -> None:
        self.COMBINED_SCHEMA = {
            "id": pl.Utf8,
            "count": pl.Int64,
            "address": pl.Struct(
                [pl.Field("line1", pl.Utf8), pl.Field("postcode", pl.Utf8)]
            ),
            "contacts": pl.List(
                pl.Struct([pl.Field("name", pl.Utf8), pl.Field("phone", pl.Utf8)])
            ),
        }

    def test_empty_generator_returns_empty_df_with_schema(self):
        schema = {"id": pl.Utf8, "count": pl.Int64}
        df = cqc.build_dataframe_from_api(self.gen(), schema)
        pl_testing.assert_frame_equal(df, pl.DataFrame(schema=schema))

    def test_normalises_scalar_struct_and_list_columns(self):
        """
        Covers in one pass:
        - scalar: present value, missing value (→ null)
        - struct: full match, missing field (→ null), null value (→ null struct), extra field stripped
        - list-of-struct: full match, missing inner field (→ null), null/empty list (→ [])
        """
        rows = [
            {  # all fields fully populated
                "id": "1",
                "count": 10,
                "address": {"line1": "A", "postcode": "EC1"},
                "contacts": [{"name": "Alice", "phone": "111"}],
            },
            {  # missing scalar, missing struct field, missing inner list field
                "id": "2",
                "address": {"line1": "B"},
                "contacts": [{"name": "Bob"}],
            },
            {  # null struct → null struct fields, null list → empty list
                "id": "3",
                "count": 30,
                "address": None,
                "contacts": None,
            },
            {  # extra struct field stripped, empty list preserved
                "id": "4",
                "count": 40,
                "address": {"line1": "D", "postcode": "N1", "country": "UK"},
                "contacts": [],
            },
        ]
        df = cqc.build_dataframe_from_api(self.gen(*rows), self.COMBINED_SCHEMA)

        expected = pl.DataFrame(
            {
                "id": ["1", "2", "3", "4"],
                "count": [10, None, 30, 40],
                "address": [
                    {"line1": "A", "postcode": "EC1"},
                    {"line1": "B", "postcode": None},
                    {"line1": None, "postcode": None},
                    {"line1": "D", "postcode": "N1"},
                ],
                "contacts": [
                    [{"name": "Alice", "phone": "111"}],
                    [{"name": "Bob", "phone": None}],
                    [],
                    [],
                ],
            },
            schema=self.COMBINED_SCHEMA,
        )
        pl_testing.assert_frame_equal(
            df.select(list(self.COMBINED_SCHEMA.keys())),
            expected,
            check_column_order=False,
        )

    def test_extra_api_columns_preserved_alongside_schema_columns(self):
        """Columns not in schema pass through untouched; schema columns are still correct."""
        rows = [
            {
                "id": "1",
                "count": 1,
                "address": {"line1": "A", "postcode": "EC1"},
                "contacts": [],
                "new_col": "foo",
                "another": 99,
            },
            {
                "id": "2",
                "count": 2,
                "address": {"line1": "B", "postcode": "N1"},
                "contacts": [],
                "new_col": "bar",
                "another": 100,
            },
        ]
        df = cqc.build_dataframe_from_api(self.gen(*rows), self.COMBINED_SCHEMA)

        pl_testing.assert_series_equal(
            df["new_col"], pl.Series("new_col", ["foo", "bar"])
        )
        pl_testing.assert_series_equal(df["another"], pl.Series("another", [99, 100]))
        pl_testing.assert_frame_equal(
            df.select(["id", "count"]),
            pl.DataFrame(
                {"id": ["1", "2"], "count": [1, 2]},
                schema={"id": pl.Utf8, "count": pl.Int64},
            ),
        )

    def test_schema_drift_logged_once_per_new_column(self):
        schema = {"id": pl.Utf8, "count": pl.Int64}
        rows = [
            {"id": str(i), "count": i, "drift_col": i, "other_new": "x"}
            for i in range(5)
        ]

        buf = io.StringIO()
        with redirect_stdout(buf):
            cqc.build_dataframe_from_api(self.gen(*rows), schema)

        output = buf.getvalue()
        self.assertIn("[schema drift]", output)
        self.assertEqual(output.count("drift_col"), 1)
        self.assertEqual(output.count("other_new"), 1)


class DaysToRollbackStartTimestampTest(unittest.TestCase):
    def test_expected_value(self):
        self.assertEqual(cqc.days_to_rollback_start_timestamp, 30)


def backoff_schedule(retry: Retry) -> list[float]:
    """Returns the sleep urllib3 applies before each retry, using its own schedule."""
    history = RequestHistory("GET", "/", None, None, None)
    return [
        retry.new(history=(history,) * attempt).get_backoff_time()
        for attempt in range(1, retry.total + 1)
    ]


@dataclass
class RetryBudgetTestCase:
    id: str
    budget_seconds: int

    def as_pytest_param(self):
        return pytest.param(self.budget_seconds, id=self.id)


retry_budget_cases = [
    RetryBudgetTestCase(id="five_minutes", budget_seconds=300),
    RetryBudgetTestCase(id="fifteen_minutes", budget_seconds=900),
    RetryBudgetTestCase(id="thirty_minutes", budget_seconds=1800),
]


class TestBuildRetryStrategy:
    @pytest.mark.parametrize(
        "budget_seconds", [c.as_pytest_param() for c in retry_budget_cases]
    )
    def test_total_retries_cover_the_budget(self, budget_seconds: int):
        retry = cqc.build_retry_strategy(budget_seconds)

        assert sum(backoff_schedule(retry)) >= budget_seconds

    @pytest.mark.parametrize(
        "budget_seconds", [c.as_pytest_param() for c in retry_budget_cases]
    )
    def test_overshoot_is_below_backoff_max(self, budget_seconds: int):
        retry = cqc.build_retry_strategy(budget_seconds)

        overshoot = sum(backoff_schedule(retry)) - budget_seconds
        assert overshoot < retry.backoff_max

    def test_backoff_is_capped_at_two_minutes(self):
        retry = cqc.build_retry_strategy(1800)

        assert max(backoff_schedule(retry)) == 120

    @patch("time.sleep")
    def test_retry_after_header_does_not_replace_backoff(self, sleep_mock: Mock):
        retry = cqc.build_retry_strategy(1800)

        retry.sleep(Mock(headers={"Retry-After": "1000"}))

        sleep_mock.assert_not_called()


class TestGetRetryBudgetSeconds:
    def test_returns_30_minutes_when_env_var_unset(self, monkeypatch):
        monkeypatch.delenv("CQC_API_RETRY_BUDGET_SECONDS", raising=False)

        assert cqc.get_retry_budget_seconds() == 1800

    def test_returns_env_var_value_when_set(self, monkeypatch):
        monkeypatch.setenv("CQC_API_RETRY_BUDGET_SECONDS", "600")

        assert cqc.get_retry_budget_seconds() == 600

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("0", id="zero"),
            pytest.param("-5", id="negative"),
            pytest.param("ten", id="not_a_number"),
            pytest.param("86401", id="too_large"),
            pytest.param("\u00b2", id="superscript_digit"),
        ],
    )
    def test_raises_when_env_var_is_not_a_positive_integer(self, monkeypatch, value):
        monkeypatch.setenv("CQC_API_RETRY_BUDGET_SECONDS", value)

        with pytest.raises(ValueError, match="integer between 1 and 86400"):
            cqc.get_retry_budget_seconds()


if __name__ == "__main__":
    unittest.main()
