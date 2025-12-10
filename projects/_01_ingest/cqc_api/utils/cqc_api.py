from typing import Generator, Iterable, List

import polars as pl
import requests
from ratelimit import limits, sleep_and_retry
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError, ResponseError
from urllib3.util.retry import Retry

CQC_API_VERSION = "v1"
RATE_LIMIT = 2000
ONE_MINUTE = 60
DEFAULT_PAGE_SIZE = 500
CQC_API_BASE_URL = "https://api.service.cqc.org.uk"
USER_AGENT = "SkillsForCare"


class NoProviderOrLocationException(Exception):
    pass


RETRY_STRATEGY = Retry(
    total=8,
    backoff_factor=0.25,
    status_forcelist=[429, 500, 502, 503, 504],
)
CQC_ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=ONE_MINUTE)
def call_api(
    url: str, query_params: dict | None = None, headers_dict: dict | None = None
) -> dict:
    """
    Calls an API and returns the json response
    Args:
        url (str): the api url
        query_params (dict | None): the parameters to pass to the api
        headers_dict (dict | None): headers to pass to the api

    Returns:
        dict: the json response

    Raises:
        NoProviderOrLocationException: if the api is unavailable
        Exception: if the api returns an unexpected code
    """
    with requests.Session() as session:
        try:
            session.mount(CQC_API_BASE_URL, CQC_ADAPTER)
            response = session.get(url, params=query_params, headers=headers_dict)
        except (MaxRetryError, ResponseError) as e:
            raise Exception("Max retries exceeded: {}".format(e))

    if (response.status_code == 403) & (headers_dict is None):
        raise Exception(
            "API response: {}, ensure you have set a User-Agent header".format(
                response.status_code
            )
        )
    if response.status_code == 404:
        raise NoProviderOrLocationException(
            f"API response: {response.status_code} - {response.json().get('message')}"
        )

    if response.status_code != 200:
        raise Exception(
            "API response: {} - {}".format(response.status_code, response.text)
        )

    return response.json()


def get_all_objects(
    object_type: str,
    object_identifier: str,
    cqc_api_primary_key: str,
    per_page: int = DEFAULT_PAGE_SIZE,
) -> Iterable[List[dict]]:
    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/{object_type}"

    total_pages = call_api(
        url,
        {
            "perPage": per_page,
        },
        headers_dict={
            "User-Agent": USER_AGENT,
            "Ocp-Apim-Subscription-Key": cqc_api_primary_key,
        },
    )["totalPages"]

    print(f"Total pages: {total_pages}")
    print(f"Beginning CQC bulk download of {object_type}...")

    for page_number in range(1, total_pages + 1):
        print(f"Collecting {object_type} from API page {page_number}/{total_pages}")
        page_locations = get_page_objects(
            url, page_number, object_type, object_identifier, cqc_api_primary_key
        )

        yield page_locations


def get_page_objects(
    url,
    page_number,
    object_type,
    object_identifier,
    cqc_api_primary_key,
    per_page=DEFAULT_PAGE_SIZE,
) -> List[dict]:
    page_objects = []
    response_body = call_api(
        url,
        {"page": page_number, "perPage": per_page},
        headers_dict={
            "User-Agent": USER_AGENT,
            "Ocp-Apim-Subscription-Key": cqc_api_primary_key,
        },
    )

    for resource in response_body[object_type]:
        returned_object = get_object(
            resource[object_identifier], object_type, cqc_api_primary_key
        )
        page_objects.append(returned_object)

    return page_objects


def get_object(cqc_location_id, object_type, cqc_api_primary_key) -> dict:
    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/{object_type}/"
    response = call_api(
        url + cqc_location_id,
        query_params=None,
        headers_dict={
            "User-Agent": USER_AGENT,
            "Ocp-Apim-Subscription-Key": cqc_api_primary_key,
        },
    )
    return response


def get_updated_objects(
    object_type: str,
    organisation_type: str,
    cqc_api_primary_key: str,
    start_timestamp: str,
    end_timestamp: str,
    per_page: int = DEFAULT_PAGE_SIZE,
) -> Generator[dict, None, None]:
    """Gets all objects of a given type that have been updated within a specified timeframe.

    Args:
        object_type (str): the type of object to retrive: one of 'providers', 'locations'
        organisation_type (str): the URL organisationType key for the object, e.g. 'provider' or 'location'
        cqc_api_primary_key (str): the CQC API key
        start_timestamp (str): the start datetime for the timeframe in ISO 8601 format (e.g. '2023-01-01T00:00:00Z')
        end_timestamp (str): the end datetime for the timeframe in ISO 8601 format (e.g. '2023-01-02T00:00:00Z')
        per_page (int): the number of organisation objects to fetch per page, defaults to `DEFAULT_PAGE_SIZE`

    Yields:
        dict: all of the objects of the specified type which contain updated data for the specified timeframe.
    """
    page_number = 1
    while True:
        total_pages = -1
        changes_by_page = get_changes_within_timeframe(
            organisation_type=organisation_type,
            cqc_api_primary_key=cqc_api_primary_key,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            page_number=page_number,
            per_page=per_page,
        )
        print(f"Changes for page {page_number}: {changes_by_page}")

        # Get total pages from first query
        if total_pages == -1:
            total_pages = changes_by_page["totalPages"]
            print(f"Total pages to search for changes: {total_pages}")

        if total_pages == 0:
            print(
                f"No {organisation_type}s updated between {start_timestamp} and {end_timestamp}"
            )
            return

        for id in changes_by_page["changes"]:
            try:
                # return each object within a generator
                yield get_object(id, object_type, cqc_api_primary_key)
            except NoProviderOrLocationException as err:
                # CQC API changes URL returns unfetchable providerIds
                print(err)
                print(f"Unable to fetch data for providerId: {id}")

        if changes_by_page["page"] == total_pages:
            print("Completed final page of changes.")
            break

        print(f"Querying page {page_number + 1} of {total_pages}.")
        page_number += 1


def get_changes_within_timeframe(
    organisation_type: str,
    cqc_api_primary_key: str,
    start_timestamp: str,
    end_timestamp: str,
    page_number: int,
    per_page: int,
) -> dict:
    """Gets changes within a specified timeframe for a given object type.

    Args:
        organisation_type (str): the type of object to retrive: one of 'provider', 'location'
        cqc_api_primary_key (str): the CQC API key
        start_timestamp (str): _start date for the timeframe in ISO 8601 format (e.g. '2023-01-01T00:00:00Z')
        end_timestamp (str): _end date for the timeframe in ISO 8601 format (e.g. '2023-01-02T00:00:00Z')
        page_number (int): the page number to fetch from the API
        per_page (int, optional): the page number from CQC's full page list. Defaults to DEFAULT_PAGE_SIZE.

    Returns:
        dict: the repsonse from the CQC changes API, containing a list of changes and pagination information.
    """

    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/changes/{organisation_type}"

    response = call_api(
        url,
        query_params={
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
            "page": page_number,
            "perPage": per_page,
        },
        headers_dict={
            "User-Agent": USER_AGENT,
            "Ocp-Apim-Subscription-Key": cqc_api_primary_key,
        },
    )
    return response


def normalise_structs(record: dict, schema: dict) -> dict:
    """
    Normalises all struct-type columns in a record so they strictly match the
    provided Polars schema.

    For any column defined as `pl.Struct` in the schema:
      - Only fields defined in the schema are retained.
      - Missing fields are added with value None.
      - If the column is missing or the value is not a dict, a new struct with
        all schema fields set to None is created.

    Args:
        record (dict): A single API record represented as a dictionary.
        schema (dict): A mapping of column names to Polars data types.

    Returns:
        dict: A new record where all struct-type columns match the schema
        exactly, with no extra or missing fields.
    """
    fixed = dict(record)

    for col, dtype in schema.items():
        if isinstance(dtype, pl.Struct):
            schema_fields = [field.name for field in dtype.fields]

            # Missing struct or wrong type → create blank struct
            if col not in fixed or not isinstance(fixed[col], dict):
                fixed[col] = {name: None for name in schema_fields}
                continue

            # Keep only schema-defined fields, fill missing ones
            fixed[col] = {name: fixed[col].get(name, None) for name in schema_fields}

    return fixed


def primed_generator(
    api_generator: Generator[dict, None, None], schema: dict
) -> Generator[dict, None, None]:
    """
    Yields one fully-normalised, schema-perfect empty row first so that
    Polars infers the schema correctly. Then yields all normalised API rows.

    This guarantees Polars never infers incorrect struct widths (e.g. struct[6]
    vs struct[3]) caused by inconsistent API responses.

    Args:
        api_generator (Generator[dict, None, None]): A generator yielding raw
            API rows as dictionaries.
        schema (dict): Polars schema mapping column names to data types.

    Yields:
        dict: A priming row followed by fully-normalised API rows.
    """
    empty_row = {}

    for col, dtype in schema.items():
        if isinstance(dtype, pl.Struct):
            empty_row[col] = {field.name: None for field in dtype.fields}
        else:
            empty_row[col] = None

    yield empty_row

    for row in api_generator:
        yield normalise_structs(row, schema)
