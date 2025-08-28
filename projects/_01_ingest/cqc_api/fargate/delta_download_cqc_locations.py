"""Retrieves Location data from the CQC API."""

from utils.aws_secrets_manager_utilities import get_secret
import os
import json
from datetime import datetime as dt
from datetime import date
import logging
from projects._01_ingest.cqc_api.utils import cqc_api as cqc
import polars as pl
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as ColNames,
)
from polars_utils import utils
from typing import Generator
from argparse import ArgumentError, ArgumentTypeError
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

ISO_8601_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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

        logger.info(f'Getting SecretID "{SECRET_ID}"')
        secret: str = get_secret(secret_name=SECRET_ID, region_name=AWS_REGION)
        cqc_api_primary_key_value: str = json.loads(secret)["Ocp-Apim-Subscription-Key"]

        logger.info("Collecting locations with changes from API")
        generator = cqc.get_updated_objects(
            object_type=CQC_OBJECT_TYPE,
            organisation_type=CQC_ORG_TYPE,
            cqc_api_primary_key=cqc_api_primary_key_value,
            start_timestamp=f"{start_dt.isoformat(timespec='seconds')}Z",
            end_timestamp=f"{end_dt.isoformat(timespec='seconds')}Z",
        )

        logger.info("Creating dataframe and writing to Parquet")
        df: pl.DataFrame = pl.DataFrame(generator, POLARS_LOCATION_SCHEMA)
        df_unique: pl.DataFrame = df.unique(subset=[ColNames.location_id])

        output_file_path = f"{destination}data.parquet"
        utils.write_to_parquet(df_unique, output_file_path, logger=logger)
        return None

    except InvalidTimestampArgumentError as e:
        logger.error(e)
        logger.error(sys.argv)
        raise
    except FileNotFoundError as e:
        logger.error(e)
        logger.error(sys.argv)
        raise
    except Exception as e:
        logger.error(e)
        raise


if __name__ == "__main__":
    try:
        destination_prefix, start, end, *_ = utils.collect_arguments(
            (
                "--destination_prefix",
                "Source s3 directory for parquet CQC locations dataset",
                False,
            ),
            ("--start_timestamp", "Start timestamp for location changes", True),
            ("--end_timestamp", "End timestamp for location changes", True),
        )
        logger.info(f"Running job from {start} to {end}")

        date_today = date.today()
        date_fp = utils.generate_s3_datasets_dir_date_path(
            destination_prefix=destination_prefix,
            domain="CQC_delta",
            dataset="delta_locations_api",
            date=date_today,
            version="3.0.0",
        )

        main(destination=date_fp, start_timestamp=start, end_timestamp=end)

    except (ArgumentError, ArgumentTypeError) as e:
        logger.error(f"An error occurred parsing arguments for {sys.argv}")
        raise e
