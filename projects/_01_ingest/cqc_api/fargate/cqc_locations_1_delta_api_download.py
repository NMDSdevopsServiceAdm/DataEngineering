"""Retrieves Location data from the CQC API."""

import json
import os
import sys
from datetime import date
from datetime import datetime as dt

import polars as pl

from polars_utils import utils
from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
from utils.aws_secrets_manager_utilities import get_secret
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as ColNames,
)


class InvalidTimestampArgumentError(Exception):
    pass


SECRET_ID = os.environ.get("CQC_SECRET_NAME", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
CQC_OBJECT_TYPE = "locations"
CQC_ORG_TYPE = "location"


def main(destination: str, start_timestamp: str, end_timestamp: str) -> None:
    """
    This function performs the following steps:
    1. Validates the provided start and end timestamps.
    2. Retrieves the CQC API subscription key from AWS Secrets Manager.
    3. Calls the CQC API to fetch updated location objects within the specified
       time range.
    4. Converts the retrieved data into a Polars DataFrame, applying a predefined
       schema.
    5. Removes duplicate location entries, keeping only unique locations.
    6. Writes the unique location data to a Parquet file at the specified
       destination path, typically an S3 location.

    Args:
        destination (str): The S3 path or local file path where the processed
            Parquet file will be written.
        start_timestamp (str): The ISO 8601 formatted string representing the
            start of the data retrieval period (e.g., '2023-01-01T00:00:00Z').
        end_timestamp (str): The ISO 8601 formatted string representing the
            end of the data retrieval period (e.g., '2023-01-31T23:59:59Z').

    Return:
        None

    Raises:
        InvalidTimestampArgumentError: If `start_timestamp` is after `end_timestamp`.
        FileNotFoundError: If the function is unable to write the Parquet
            file to the specified `destination`.
        Exception: For any other unspecified errors that occur during API
            calls, secret retrieval, or data processing.
    """
    try:
        destination = destination if destination[-1] == "/" else f"{destination}/"

        start_dt = dt.fromisoformat(start_timestamp.replace("Z", ""))
        end_dt = dt.fromisoformat(end_timestamp.replace("Z", ""))

        if start_dt > end_dt:
            raise InvalidTimestampArgumentError(
                "start_timestamp is after end_timestamp"
            )

        print(f'Getting SecretID "{SECRET_ID}"')
        secret = get_secret(secret_name=SECRET_ID, region_name=AWS_REGION)
        cqc_api_primary_key_value: str = json.loads(secret)["Ocp-Apim-Subscription-Key"]

        print("Collecting locations with changes from API")
        generator = cqc.get_updated_objects(
            object_type=CQC_OBJECT_TYPE,
            organisation_type=CQC_ORG_TYPE,
            cqc_api_primary_key=cqc_api_primary_key_value,
            start_timestamp=f"{start_dt.isoformat(timespec='seconds')}Z",
            end_timestamp=f"{end_dt.isoformat(timespec='seconds')}Z",
        )

        print("Creating dataframe and writing to Parquet")
        df: pl.DataFrame = pl.DataFrame(generator)
        df_unique: pl.DataFrame = df.unique(subset=[ColNames.location_id])

        utils.write_to_parquet(df_unique, destination)
        return None

    except InvalidTimestampArgumentError as e:
        print(f"ERROR: {e}")
        print(f"ERROR: {sys.argv}")
        raise
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print(f"ERROR: {sys.argv}")
        raise
    except Exception as e:
        print(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--destination_prefix",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        ("--start_timestamp", "Start timestamp for location changes"),
        ("--end_timestamp", "End timestamp for location changes"),
    )
    print(f"Running job from {args.start_timestamp} to {args.end_timestamp}")

    date_today = date.today()
    destination = utils.generate_s3_dir(
        destination_prefix=args.destination_prefix,
        domain="CQC_delta",
        dataset="delta_locations_api",
        date=date_today,
        version="3.1.0",
    )
    main(destination, args.start_timestamp, args.end_timestamp)
