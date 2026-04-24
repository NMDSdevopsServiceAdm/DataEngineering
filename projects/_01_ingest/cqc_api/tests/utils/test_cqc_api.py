import io
import unittest
from contextlib import redirect_stdout
from http.client import HTTPMessage
from typing import Generator
from unittest.mock import Mock, call, patch

import polars as pl
import polars.testing as pl_testing
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    @patch("requests.Session.get")
    def test_call_api_handles_200(self, get_mock: Mock):
        test_response = TestResponse(200, {})
        get_mock.return_value = test_response

        response_json = cqc.call_api(
            self.test_url, {"test": "body"}, headers_dict={"some": "header"}
        )
        self.assertEqual(response_json, {})

    @patch("requests.Session.get")
    def test_call_api_handles_500(self, get_mock: Mock):
        test_response = TestResponse(500, {})
        get_mock.return_value = test_response

        with self.assertRaisesRegex(Exception, "^API response: 500 -.*"):
            cqc.call_api(
                self.test_url, {"test": "body"}, headers_dict={"some": "header"}
            )

    @patch("requests.Session.get")
    def test_call_api_handles_400(self, get_mock: Mock):
        test_response = TestResponse(400, {})
        get_mock.return_value = test_response

        with self.assertRaisesRegex(Exception, "^API response: 400 -.*"):
            cqc.call_api(
                self.test_url, {"test": "body"}, headers_dict={"some": "header"}
            )

    @patch("requests.Session.get")
    def test_call_api_handles_404(self, get_mock: Mock):
        test_response = TestResponse(404, {})
        get_mock.return_value = test_response

        with self.assertRaisesRegex(
            cqc.NoProviderOrLocationException, "^API response: 404 -.*"
        ):
            cqc.call_api(
                self.test_url, {"test": "body"}, headers_dict={"some": "header"}
            )

    @patch("requests.Session.get")
    def test_call_api_handles_403_with_headers(self, get_mock: Mock):
        test_response = TestResponse(403, {})
        get_mock.return_value = test_response

        with self.assertRaisesRegex(Exception, "^API response: 403 -.*"):
            cqc.call_api(
                self.test_url, {"test": "body"}, headers_dict={"some": "header"}
            )

    @patch("requests.Session.get")
    def test_call_api_handles_403_without_headers(self, get_mock: Mock):
        test_response = TestResponse(403, {})
        get_mock.return_value = test_response

        with self.assertRaises(Exception) as context:
            cqc.call_api(self.test_url, {"test": "body"}, headers_dict=None)

        self.assertTrue(
            "API response: 403, ensure you have set a User-Agent header"
            in str(context.exception)
        )

    @patch("requests.sessions.SessionRedirectMixin.resolve_redirects")
    @patch("requests.adapters.HTTPAdapter.build_response")
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_retry_on_server_error(self, getconn_mock, response_mock, _):
        # Given
        response_500 = Mock(name="conn_response_500")
        response_500.status = 500
        response_500.headers = {"Retry-After": "1"}
        response_500.msg = HTTPMessage()

        response_200 = Mock(name="conn_response_200")
        response_200.status = 200
        response_200.headers = {"Retry-After": "1"}
        response_200.msg = HTTPMessage()

        getconn_mock.side_effect = [response_500, response_500, response_200]

        response_mock.return_value = Mock(name="mock_response_200", history=False)
        response_mock.return_value.status_code = 200
        response_mock.return_value.json.return_value = {"data": "success"}

        # When
        result = cqc.call_api(
            url="https://api.service.cqc.org.uk/test",
            query_params={"param": "value"},
        )
        # Then
        assert result == {"data": "success"}
        assert getconn_mock.call_count == 3  # only 3 calls as 3rd should succeed

    @patch(f"{PATCH_PATH}.CQC_ADAPTER", PATCHED_CQC_ADAPTER)
    @patch("requests.sessions.SessionRedirectMixin.resolve_redirects")
    @patch("requests.adapters.HTTPAdapter.build_response")
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_max_retries_on_server_error(self, getconn_mock, response_mock, _):
        # Given
        response_500 = Mock(name="conn_response_500")
        response_500.status = 500
        response_500.headers = {"Retry-After": "1"}
        response_500.msg = HTTPMessage()

        response_200 = Mock(name="conn_response_200")
        response_200.status = 200
        response_200.headers = {"Retry-After": "1"}
        response_200.msg = HTTPMessage()

        getconn_mock.side_effect = [
            response_500,
            response_500,
            response_500,
            response_500,
            response_200,
        ]

        response_mock.return_value = Mock(name="mock_response_200", history=False)
        response_mock.return_value.status_code = 200
        response_mock.return_value.json.return_value = {"data": "success"}

        response_mock.return_value = {"data": "success"}

        # When
        with self.assertRaises(Exception) as context:
            result = cqc.call_api(
                url="https://api.service.cqc.org.uk/test",
                query_params={"param": "value"},
            )
            # Then
            self.assertIsNone(result)
            self.assertTrue("Max retries exceeded" in str(context.exception))

        assert (
            getconn_mock.call_count == 4
        )  # only 4 successful calls as 5th should fail


class GetAllObjectsTests(CqcApiTests):
    @patch(f"{PATCH_PATH}.get_page_objects")
    @patch(f"{PATCH_PATH}.call_api")
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
        self.assertEqual(cqc.days_to_rollback_start_timestamp, "15")


if __name__ == "__main__":
    unittest.main()
