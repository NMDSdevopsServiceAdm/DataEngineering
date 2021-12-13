
from schemas.cqc_location_schema import LOCATION_SCHEMA
from pyspark import SparkConf
from pyspark.context import SparkContext
from pprint import pprint
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from ratelimit import limits, sleep_and_retry
from time import sleep

import requests

CQC_API_VERSION = "v1"
RATE_LIMIT = 550
ONE_MINUTE = 60
DEFAULT_PAGE_SIZE = 50
OUTPUT_DIR = "cqc_locations.parquet"
S3_DIR_ROOT = "s3://sfc-data-engineering/domain=CQC/domain=locations-api/version=1.0.0/"


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=ONE_MINUTE)
def call_api(url, query_params=None):
    response = requests.get(url, query_params)

    if response.status_code != 200:
        raise Exception("API response: {}".format(response.status_code))

    return response.json()


def get_all_locations(stream, per_page=DEFAULT_PAGE_SIZE):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/locations"

    total_pages = call_api(url, {"perPage": PER_PAGE})["totalPages"]
    all_locations = []

    print(f"Total pages: {total_pages}")
    print(f"Beginning CQC bulk download of locations...")

    for page_number in range(1, total_pages):
        print(
            f"Collecting locations from API page {page_number}/{total_pages}")
        page_locations = get_page_locations(url, page_number)

        if stream:
            yield page_locations
        else:
            all_locations.append(page_locations)

    if not stream:
        return all_locations


def get_page_locations(url, page_number, per_page=DEFAULT_PAGE_SIZE):

    page_locations = []
    response_body = call_api(url, {"page": page_number, "perPage": per_page})

    for cqc_location in response_body["locations"]:
        location = get_location(cqc_location["locationId"])
        page_locations.append(location)

    return page_locations


def get_location_change_list(min_date, max_date):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/changes/location?startTimestamp={min_date}&endTimestamp={max_date}"
    change_list = call_api(url)["changes"]

    return change_list


def get_location(cqc_location_id):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/locations/"

    location_body = call_api(url + cqc_location_id)
    return location_body
