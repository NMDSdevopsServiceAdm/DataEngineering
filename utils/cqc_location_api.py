"""
https://anypoint.mulesoft.com/exchange/portals/care-quality-commission-5/4d36bd23-127d-4acf-8903-ba292ea615d4/cqc-syndication-1/minor/1.0/pages/home/
In order to provide CQC's public data services we ask that all organisations consuming this API add an additional query parameter named partnerCode to all requests. If you are a CQC Syndication partner the value for this code will be provided to you, otherwise an informative but concise code representing your organisation should be chosen. For example:

https://api.cqc.org.uk/public/v1/providers?perPage=1000&partnerCode=NHSCHOICES
https://api.cqc.org.uk/public/v1/providers/1-1016936648?partnerCode=NHSCHOICES

Rate Limiting
This API will limit the overall steady-state request rate to 3000 requests per minute per API operation.
The API is additionally limited to 600 requests per minute for each client.
Higher request rates may be subject to throttling.
An API request that has been throttled will result in a HTTP response containing a status code 429 "Too Many Requests".

https://api.cqc.org.uk/public/v1/changes/location?startTimestamp=2021-01-01T06:30:00Z&endTimestamp=2021-01-02T06:30:00Z

"""


from pprint import pprint
from ratelimit import limits, sleep_and_retry
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from cqc_location_schema import LOCATION_SCHEMA

import requests

CQC_API_VERSION = "v1"
RATE_LIMIT = 55
SIX_SECONDS = 6


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=SIX_SECONDS)
def call_api(url, query_params=None):
    response = requests.get(url, query_params)

    if response.status_code != 200:
        raise Exception("API response: {}".format(response.status_code))
    return response


def get_all_locations():
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/locations"

    total_pages = requests.get(url, {"perPage": 10}).json()["totalPages"]
    locations = []

    print(f"Total pages: {total_pages}")
    print(f"Beginning CQC bulk download of locations...")

    for page_count in range(1, 2):
        print(f"Collect page {page_count}/{total_pages} locations")

        response = call_api(url, {"page": page_count, "perPage": 10})
        response_body = response.json()

        for cqc_location in response_body["locations"]:
            location = get_location(cqc_location["locationId"])
            locations.append(location)

    return locations


def get_location_change_list(min_date, max_date):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/changes/location?startTimestamp={min_date}&endTimestamp={max_date}"
    changes_response = call_api(url)
    change_list = changes_response.json()["changes"]

    return change_list


def get_location(cqc_location_id):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/locations/"

    location_response = call_api(url + cqc_location_id)
    location_body = location_response.json()

    return location_body


def write_to_parquet(data):
    spark = SparkSession.builder \
        .appName("sfc_data_engineering_pull_cqc_locations") \
        .getOrCreate()

    print("Creating dataframe")
    df = spark.createDataFrame(data, LOCATION_SCHEMA)
    print("Showing dataframe")
    df.show()
    print("Writing to Parquet")
    df.write.parquet("cqc_locations.parquet")


def main():
    print("Collecting all locations from API")
    # locations = get_all_locations()
    location = get_location("1-10000792582")
    write_to_parquet([location])


if __name__ == "__main__":
    main()
