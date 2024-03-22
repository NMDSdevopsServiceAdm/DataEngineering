from ratelimit import limits, sleep_and_retry
from time import sleep

import requests

CQC_API_VERSION = "v1"
RATE_LIMIT = (
    400  # Max ratelimit = 600 per minute without partnercode, 2000 with a partnercode
)
ONE_MINUTE = 60
DEFAULT_PAGE_SIZE = 500


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=ONE_MINUTE)
def call_api(url, query_params=None):
    response = requests.get(url, query_params)

    while response.status_code == 429:
        print("Sleeping for ten seconds due to rate limiting")
        sleep(10)
        response = requests.get(url, query_params)

    if response.status_code != 200:
        raise Exception("API response: {}".format(response.status_code))

    return response.json()


def get_all_objects(
    stream,
    object_type,
    object_identifier,
    partner_code,
    per_page=DEFAULT_PAGE_SIZE,
):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/{object_type}"

    total_pages = call_api(url, {"perPage": per_page, "partnerCode": partner_code})[
        "totalPages"
    ]
    all_objects = []

    print(f"Total pages: {total_pages}")
    print(f"Beginning CQC bulk download of {object_type}...")

    for page_number in range(1, total_pages + 1):
        print(f"Collecting {object_type} from API page {page_number}/{total_pages}")
        page_locations = get_page_objects(
            url, page_number, object_type, object_identifier, partner_code
        )

        if stream:
            yield page_locations
        else:
            all_objects.append(page_locations)

    if not stream:
        return all_objects


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
        url, {"page": page_number, "perPage": per_page, "partnerCode": partner_code}
    )

    for resource in response_body[object_type]:
        returned_object = get_object(
            resource[object_identifier], object_type, partner_code
        )
        page_objects.append(returned_object)

    return page_objects


def get_object(cqc_location_id, object_type, partner_code):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/{object_type}/"

    location_body = call_api(url + cqc_location_id, {"partnerCode": partner_code})
    return location_body
