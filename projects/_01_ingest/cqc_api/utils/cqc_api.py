import logging
import time
from typing import Iterator

import requests
from ratelimit import limits, sleep_and_retry


CQC_API_VERSION = "v1"
RATE_LIMIT = 2000
ONE_MINUTE = 60
DEFAULT_PAGE_SIZE = 500
CQC_API_BASE_URL = "https://api.service.cqc.org.uk"
USER_AGENT = "SkillsForCare"


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=ONE_MINUTE)
def call_api(url, query_params=None, headers_dict=None):
    response = requests.get(url, query_params, headers=headers_dict)

    while response.status_code == 429:
        print("Sleeping for ten seconds due to rate limiting")
        time.sleep(10)
        response = requests.get(url, query_params, headers=headers_dict)

    if (response.status_code == 403) & (headers_dict is None):
        raise Exception(
            "API response: {}, ensure you have set a User-Agent header".format(
                response.status_code
            )
        )

    if response.status_code != 200:
        raise Exception("API response: {}".format(response.status_code))

    return response.json()


def get_all_objects(
    object_type: str,
    object_identifier: str,
    cqc_api_primary_key: str,
    per_page=DEFAULT_PAGE_SIZE,
):
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
):
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


def get_object(cqc_location_id, object_type, cqc_api_primary_key):
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
    cqc_api_primary_key: str,
    start: str,
    end: str,
) -> Iterator:
    """Gets all objects of a given type that have been updated within a specified timeframe.

    Args:
        object_type (str): the type of object to retrive: one of 'providers', 'locations'
        object_identifier (str): the key for the object, e.g. 'providerId' or 'locationId'
        cqc_api_primary_key (str): the CQC API key
        start (str): the start date for the timeframe in ISO 8601 format (e.g. '2023-01-01T00:00:00Z')
        end (str): the end date for the timeframe in ISO 8601 format (e.g. '2023-01-02T00:00:00Z')

    Yields:
        Iterator: all of the objects of the specified type which contain updated data for the specified timeframe.
    """
    page_number = 1
    while True:
        total_pages = -1
        changes_by_page = get_changes_within_timeframe(
            object_type=object_type,
            cqc_api_primary_key=cqc_api_primary_key,
            start=start,
            end=end,
            page_number=page_number,
        )
        logging.debug(f"Changes for page {page_number}: {changes_by_page}")

        # Get total pages from first query
        if total_pages == -1:
            total_pages = changes_by_page["totalPages"]
            logging.info(f"Total pages to search for changes: {total_pages}")

        for id in changes_by_page["changes"]:
            # return each object within a generator
            yield get_object(id, object_type, cqc_api_primary_key)

        if changes_by_page["page"] == total_pages:
            logging.info("Completed final page of changes.")
            break

        logging.info(f"Querying page {page_number + 1} of {total_pages}.")
        page_number += 1


def get_changes_within_timeframe(
    object_type: str,
    cqc_api_primary_key: str,
    start: str,
    end: str,
    page_number: int,
    per_page=DEFAULT_PAGE_SIZE,
) -> dict:
    """Gets changes within a specified timeframe for a given object type.

    Args:
        object_type (str): the type of object to retrive: one of 'providers', 'locations'
        cqc_api_primary_key (str): the CQC API key
        start (str): _start date for the timeframe in ISO 8601 format (e.g. '2023-01-01T00:00:00Z')
        end (str): _end date for the timeframe in ISO 8601 format (e.g. '2023-01-02T00:00:00Z')
        per_page (int, optional): the page number from CQC's full page list. Defaults to DEFAULT_PAGE_SIZE.

    Returns:
        dict: the repsonse from the CQC changes API, containing a list of changes and pagination information.
    """

    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/changes/{object_type}"

    response = call_api(
        url,
        query_params={
            "startTimestamp": start,
            "endTimestamp": end,
            "page": page_number,
            "perPage": per_page,
        },
        headers_dict={
            "User-Agent": USER_AGENT,
            "Ocp-Apim-Subscription-Key": cqc_api_primary_key,
        },
    )
    return response
