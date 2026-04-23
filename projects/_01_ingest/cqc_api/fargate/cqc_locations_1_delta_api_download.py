"""Retrieves Location data from the CQC API."""

import json
import os
import sys
from datetime import date
from datetime import datetime as dt
from datetime import timedelta
from typing import Generator

import polars as pl

from polars_utils import utils
from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
from utils.aws_secrets_manager_utilities import get_secret
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as ColNames,
)

SECRET_ID = os.environ.get("CQC_SECRET_NAME", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
CQC_OBJECT_TYPE = "locations"
CQC_ORG_TYPE = "location"


def main(
    destination: str,
    end_timestamp: str = None,
    previous_days_to_capture: int = cqc.days_to_rollback_start_timestamp,
) -> None:
    """
    This function performs the following steps:
    1. Subtracts a number of days from end_timestamp to create a start date.
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
        end_timestamp (str): The ISO 8601 formatted string representing the
            end of the data retrieval period (e.g., '2023-01-31T23:59:59Z').
        previous_days_to_capture (int): Number of days before end_timestamp (default 15).

    Return:
        None

    Raises:
        FileNotFoundError: If the function is unable to write the Parquet
            file to the specified `destination`.
        Exception: For any other unspecified errors that occur during API
            calls, secret retrieval, or data processing.
    """
    try:
        destination = destination if destination[-1] == "/" else f"{destination}/"

        end_dt = dt.fromisoformat(end_timestamp.replace("Z", ""))
        start_dt = end_dt - timedelta(days=previous_days_to_capture)

        print(f'Getting SecretID "{SECRET_ID}"')
        secret = get_secret(secret_name=SECRET_ID, region_name=AWS_REGION)
        cqc_api_primary_key_value: str = json.loads(secret)["Ocp-Apim-Subscription-Key"]

        print(
            f"Collecting locations with changes from API between {start_dt} and {end_dt}"
        )

        api_generator: Generator[dict, None, None] = cqc.get_updated_objects(
            object_type=CQC_OBJECT_TYPE,
            organisation_type=CQC_ORG_TYPE,
            cqc_api_primary_key=cqc_api_primary_key_value,
            start_timestamp=f"{start_dt.isoformat(timespec='seconds')}Z",
            end_timestamp=f"{end_dt.isoformat(timespec='seconds')}Z",
        )

        print("Collecting API rows and building DataFrame")
        df: pl.DataFrame = cqc.build_dataframe_from_api(
            api_generator, POLARS_LOCATION_SCHEMA
        )
        print(f"Done — {df.shape[0]} rows, {df.shape[1]} columns")
        print("Creating dataframe and writing to Parquet")

        df_schema = df.collect_schema()
        df = df.with_columns(
            [
                pl.col(k).cast(v)
                for k, v in POLARS_LOCATION_SCHEMA.items()
                if k in df_schema.keys()
            ]
        )
        df_unique: pl.DataFrame = df.unique(subset=[ColNames.location_id]).filter(
            pl.col(ColNames.location_id).is_not_null()
        )

        utils.write_to_parquet(df_unique, destination)
        return None

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
        (
            "--end_timestamp",
            "End timestamp for location changes",
        ),
    )
    print(f"Running cqc locations delta api download job")

    date_today = date.today()
    destination = utils.generate_s3_dir(
        destination_prefix=args.destination_prefix,
        domain="CQC",
        dataset="cqc_locations_01_delta_api",
        date=date_today,
        version="3.1.0",
    )
    main(destination, end_timestamp=args.end_timestamp)
