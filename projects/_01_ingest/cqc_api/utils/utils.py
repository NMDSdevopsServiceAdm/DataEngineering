import csv

import boto3

from utils.utils import split_s3_uri


def read_manual_postcode_corrections_csv_to_dict(
    source: str, s3_client: object = None
) -> dict:
    """
    Read csv of postcode corrections from given location to a dictionary.

    Args:
        source(str): The s3 URI of the incorrect postcode csv file
        s3_client(object): An s3 client

    Returns:
        dict: A dictionary of postcode corrections in the format {incorrect: correct}
    """
    bucket, key = split_s3_uri(source)
    if s3_client is None:
        s3_client = boto3.client("s3")
    postcode_obj = s3_client.get_object(Bucket=bucket, Key=key)

    postcode_data = postcode_obj["Body"].read().decode("utf-8").splitlines()
    postcode_records = csv.reader(postcode_data)
    headers = next(postcode_records)
    postcode_dict = {record[0]: record[1] for record in postcode_records}
    return postcode_dict
