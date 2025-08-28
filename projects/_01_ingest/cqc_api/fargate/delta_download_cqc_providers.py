"""Retrieves Provider data from the CQC API."""

from utils.aws_secrets_manager_utilities import get_secret
import os
import json
from datetime import datetime as dt
from datetime import date
import logging
from projects._01_ingest.cqc_api.utils import cqc_api as cqc
import polars as pl
from schemas.cqc_provider_schema_polars import POLARS_PROVIDER_SCHEMA
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
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

SECRET_ID = os.environ.get("CQC_SECRET_NAME", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
CQC_OBJECT_TYPE = "providers"
CQC_ORG_TYPE = "provider"


class InvalidTimestampArgumentError(Exception):
    pass


def main(destination: str, start_timestamp: str, end_timestamp: str) -> None:
    """Orchestrates the retrieval of updated CQC provider data and writes it to Parquet.

    This function performs the following steps:
    1. Validates the provided start and end timestamps.
    2. Retrieves the CQC API subscription key from AWS Secrets Manager.
    3. Calls the CQC API to fetch updated provider objects within the specified
       time range.
    4. Converts the retrieved data into a Polars DataFrame, applying a predefined
       schema.
    5. Removes duplicate provider entries, keeping only unique providers.
    6. Writes the unique provider data to a Parquet file at the specified
       destination path, typically an S3 location.

    Args:
        destination (str): The S3 path or local file path where the processed
            Parquet file will be written.
        start_timestamp (str): The ISO 8601 formatted string representing the
            start of the data retrieval period (e.g., '2023-01-01T00:00:00Z').
        end_timestamp (str): The ISO 8601 formatted string representing the
            end of the data retrieval period (e.g., '2023-01-31T23:59:59Z').

    Return:
        None.

    Raises:
        InvalidTimestampArgumentError: If `start_timestamp` is after `end_timestamp`.
        FileNotFoundError: If the function is unable to write the Parquet
            file to the specified `destination`.
        Exception: For any other unspecified errors that occur during API
            calls, secret retrieval, or data processing.
    """
    logger.info("Starting Execution")
    try:
        destination = destination if destination[-1] == "/" else f"{destination}/"

        start_dt = dt.fromisoformat(start_timestamp.replace("Z", ""))
        end_dt = dt.fromisoformat(end_timestamp.replace("Z", ""))

        logger.info("Validating start and end timestamps")
        if start_dt > end_dt:
            raise InvalidTimestampArgumentError(
                "Start timestamp is after end timestamp"
            )

        logger.info(f'Getting SecretID "{SECRET_ID}"')
        secret: str = get_secret(secret_name=SECRET_ID, region_name=AWS_REGION)
        cqc_api_primary_key_value: str = json.loads(secret)["Ocp-Apim-Subscription-Key"]

        logger.info("Collecting providers with changes from API")

        generator: Generator[dict, None, None] = cqc.get_updated_objects(
            object_type=CQC_OBJECT_TYPE,
            organisation_type=CQC_ORG_TYPE,
            cqc_api_primary_key=cqc_api_primary_key_value,
            start_timestamp=f"{start_dt.isoformat(timespec='seconds')}Z",
            end_timestamp=f"{end_dt.isoformat(timespec='seconds')}Z",
        )

        logger.info("Creating dataframe and writing to Parquet")
        df: pl.DataFrame = pl.DataFrame(generator, POLARS_PROVIDER_SCHEMA)
        df_unique: pl.DataFrame = df.unique(subset=[ColNames.provider_id])
        output_file_path = f"{destination}data.parquet"
        utils.write_to_parquet(df_unique, output_file_path, logger=logger)
        return None
    except InvalidTimestampArgumentError as e:
        logger.error(f"Start timestamp is after end timestamp: Args: {sys.argv}")
        raise
    except FileNotFoundError as e:
        logger.error(
            f"{sys.argv[0]} was unable to write to destination. Args: {sys.argv}"
        )
        raise
    except Exception as e:
        logger.error(
            f"An unspecified error occurred while calling the CQC API. Args: {sys.argv}"
        )
        logger.error(e.with_traceback(e.__traceback__))
        raise


if __name__ == "__main__":
    try:
        (
            destination_prefix,
            start_timestamp,
            end_timestamp,
            *_,
        ) = utils.collect_arguments(
            (
                "--destination_prefix",
                "Source s3 directory for parquet CQC providers dataset",
                True,
            ),
            ("--start_timestamp", "Start timestamp for provider changes", True),
            ("--end_timestamp", "End timestamp for provider changes", True),
        )
        logger.info(f"Running job from {start_timestamp} to {end_timestamp}")

        todays_date = date.today()
        destination = utils.generate_s3_datasets_dir_date_path(
            destination_prefix=destination_prefix,
            domain="CQC_delta",
            dataset="delta_providers_api",
            date=todays_date,
            version="3.0.0",
        )

        main(destination, start_timestamp, end_timestamp)
    except (ArgumentError, ArgumentTypeError) as e:
        logger.error(f"An error occurred parsing arguments for {sys.argv}")
        raise e
