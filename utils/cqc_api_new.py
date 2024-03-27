from ratelimit import limits, sleep_and_retry
import time

import requests

CQC_API_VERSION = "v1"
RATE_LIMIT = (
    400  # Max ratelimit = 600 per minute without partnercode, 2000 with a partnercode
)
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

    if response.status_code not in [200]:
        raise Exception("API response: {}".format(response.status_code))

    return response.json()


def get_all_objects(
    object_type: str,
    object_identifier: str,
    partner_code: str,
    per_page=DEFAULT_PAGE_SIZE,
):
    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/{object_type}"

    total_pages = call_api(
        url,
        {"perPage": per_page, "partnerCode": partner_code},
        headers_dict={"User-Agent": USER_AGENT},
    )["totalPages"]

    print(f"Total pages: {total_pages}")
    print(f"Beginning CQC bulk download of {object_type}...")

    for page_number in range(1, total_pages + 1):
        print(f"Collecting {object_type} from API page {page_number}/{total_pages}")
        page_locations = get_page_objects(
            url, page_number, object_type, object_identifier, partner_code
        )

        yield page_locations


def get_page_objects(
    url,
    page_number,
    object_type,
    object_identifier,
    partner_code,
    per_page=DEFAULT_PAGE_SIZE,
):
    page_objects = []
    response_body = call_api(
        url,
        {"page": page_number, "perPage": per_page, "partnerCode": partner_code},
        headers_dict={"User-Agent": USER_AGENT},
    )

    for resource in response_body[object_type]:
        returned_object = get_object(resource[object_identifier], object_type)
        page_objects.append(returned_object)

    return page_objects


def get_object(cqc_location_id, object_type):
    url = f"{CQC_API_BASE_URL}/public/{CQC_API_VERSION}/{object_type}/"

    response = call_api(
        url + cqc_location_id,
        query_params=None,  # This endpoint no longer supports a Partner Code
        headers_dict={"User-Agent": USER_AGENT},
    )

    return response
