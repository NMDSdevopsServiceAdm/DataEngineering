import logging
import time
from typing import Generator, Iterable, List

import requests
from ratelimit import limits, sleep_and_retry


CQC_API_VERSION = "v1"
RATE_LIMIT = 2000
ONE_MINUTE = 60
DEFAULT_PAGE_SIZE = 500
CQC_API_BASE_URL = "https://api.service.cqc.org.uk"
USER_AGENT = "SkillsForCare"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class NoProviderOrLocationException(Exception):
    pass


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=ONE_MINUTE)
def call_api(url, query_params=None, headers_dict=None) -> dict:
    response = requests.get(url, query_params, headers=headers_dict)

    while response.status_code == 429:
        logger.info("Sleeping for ten seconds due to rate limiting")
        time.sleep(10)
        response = requests.get(url, query_params, headers=headers_dict)

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
    per_page=DEFAULT_PAGE_SIZE,
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

    logger.info(f"Total pages: {total_pages}")
    logger.info(f"Beginning CQC bulk download of {object_type}...")

    for page_number in range(1, total_pages + 1):
        logger.info(
            f"Collecting {object_type} from API page {page_number}/{total_pages}"
        )
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
) -> list[dict]:
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
    start: str,
    end: str,
    per_page: int = DEFAULT_PAGE_SIZE,
) -> Generator[dict, None, None]:
    """Gets all objects of a given type that have been updated within a specified timeframe.

    Args:
        object_type (str): the type of object to retrive: one of 'providers', 'locations'
        organisation_type (str): the URL organisationType key for the object, e.g. 'provider' or 'location'
        cqc_api_primary_key (str): the CQC API key
        start (str): the start date for the timeframe in ISO 8601 format (e.g. '2023-01-01T00:00:00Z')
        end (str): the end date for the timeframe in ISO 8601 format (e.g. '2023-01-02T00:00:00Z')
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
            start=start,
            end=end,
            page_number=page_number,
            per_page=per_page,
        )
        logging.debug(f"Changes for page {page_number}: {changes_by_page}")

        # Get total pages from first query
        if total_pages == -1:
            total_pages = changes_by_page["totalPages"]
            logger.info(f"Total pages to search for changes: {total_pages}")

        if total_pages == 0:
            logger.info(f"No {organisation_type}s updated between {start} and {end}")
            return

        for id in changes_by_page["changes"]:
            try:
                # return each object within a generator
                yield get_object(id, object_type, cqc_api_primary_key)
            except NoProviderOrLocationException as err:
                # CQC API changes URL returns unfetchable providerIds
                logger.info(err)
                logger.info(f"Unable to fetch data for providerId: {id}")

        if changes_by_page["page"] == total_pages:
            logger.info("Completed final page of changes.")
            break

        logger.info(f"Querying page {page_number + 1} of {total_pages}.")
        page_number += 1


def get_changes_within_timeframe(
    organisation_type: str,
    cqc_api_primary_key: str,
    start: str,
    end: str,
    page_number: int,
    per_page: int,
) -> dict:
    """Gets changes within a specified timeframe for a given object type.

    Args:
        organisation_type (str): the type of object to retrive: one of 'provider', 'location'
        cqc_api_primary_key (str): the CQC API key
        start (str): _start date for the timeframe in ISO 8601 format (e.g. '2023-01-01T00:00:00Z')
        end (str): _end date for the timeframe in ISO 8601 format (e.g. '2023-01-02T00:00:00Z')
        page_number (int): the page number to fetch from the API
        per_page (int, optional): the page number from CQC's full page list. Defaults to DEFAULT_PAGE_SIZE.

    Returns:
        dict: the repsonse from the CQC changes API, containing a list of changes and pagination information.
    """

    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/changes/{organisation_type}"

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
