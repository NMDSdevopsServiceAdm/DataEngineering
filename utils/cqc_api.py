from ratelimit import limits, sleep_and_retry
from time import sleep

import requests
import boto3
from botocore.exceptions import ClientError

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
            url, page_number, object_type, object_identifier
        )

        if stream:
            yield page_locations
        else:
            all_objects.append(page_locations)

    if not stream:
        return all_objects


def get_page_objects(
    url, page_number, object_type, object_identifier, per_page=DEFAULT_PAGE_SIZE
):
    page_objects = []
    response_body = call_api(url, {"page": page_number, "perPage": per_page})

    for resource in response_body[object_type]:
        returned_object = get_object(resource[object_identifier], object_type)
        page_objects.append(returned_object)

    return page_objects


def get_object(cqc_location_id, object_type):
    url = f"https://api.cqc.org.uk/public/{CQC_API_VERSION}/{object_type}/"

    location_body = call_api(url + cqc_location_id)
    return location_body


def get_secret(secret_name: str = "partner_code", region_name: str = "eu-west-2"):
    """
    A provided AWS function from the [AWS Secrets Manager](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/secrets-manager.html) python documentation for retrieving an AWS secret value.
    Leverages the [GetSecretValue](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html) request and response syntax.

    Secrets Manager decrypts the secret value using the associated KMS CMK.
    Depending on whether the secret was a string or binary, only one of the return fields will be populated.
    It is also worth noting that secrets created via the Secrets Manager console will always be a SecretString.

    Parameters:
        secret_name (str): The name of the secret as stored in AWS Secrets Manager. Defaults to be the current sole use case.
        region_name (str): The name of the region the secret is stored in. Defaults to eu-west-2 - London.

    Returns:
        text_secret_data (str): The string version of the secret value (if applicable)
        binary_secret_data (Base64-encoded binary data object): The binary version of the secret value (if applicable)
    """

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print("The requested secret " + secret_name + " was not found")
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            print("The request was invalid due to:", e)
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            print("The request had invalid params:", e)
        elif e.response["Error"]["Code"] == "DecryptionFailure":
            print(
                "The requested secret can't be decrypted using the provided KMS key:", e
            )
        elif e.response["Error"]["Code"] == "InternalServiceError":
            print("An error occurred on service side:", e)
    else:
        if "SecretString" in get_secret_value_response:
            text_secret_data = get_secret_value_response["SecretString"]
            return text_secret_data
        else:
            binary_secret_data = get_secret_value_response["SecretBinary"]
            return binary_secret_data
