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
from polars_utils import utils as polars_utils
from utils import utils as utils
from typing import Generator
from argparse import ArgumentError, ArgumentTypeError
import sys


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ISO_8601_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

SECRET_ID = os.environ.get("CQC_SECRET_NAME", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
CQC_OBJECT_TYPE = "providers"
CQC_ORG_TYPE = "provider"


def main(destination: str, start_timestamp: str, end_timestamp: str) -> None:
    try:
        start_dt = dt.fromisoformat(start_timestamp.replace("Z", ""))
        end_dt = dt.fromisoformat(end_timestamp.replace("Z", ""))

        if start_dt > end_dt:
            raise ValueError("Start timestamp is after end timestamp")

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

        df: pl.DataFrame = pl.DataFrame(generator, POLARS_PROVIDER_SCHEMA)
        df_unique: pl.DataFrame = df.unique(subset=[ColNames.provider_id])
        polars_utils.write_to_parquet(df_unique, destination, logger=logger)
        return None
    except ValueError as e:
        logger.error(f"Start timestamp is after end timestamp: Args: {sys.argv}")
        raise e
    except FileNotFoundError as e:
        logger.error(
            f"{sys.argv[0]} was unable to write to destination. Args: {sys.argv}"
        )
        raise e
    except Exception as e:
        logger.error(
            f"An unspecified error occurred while calling the CQC API. Args: {sys.argv}"
        )
        logger.error(e.with_traceback(e.__traceback__))
        raise e


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
            domain="CQC",
            dataset="providers_api",
            date=todays_date,
            version="3.0.0",
        )

        main(destination, start_timestamp, end_timestamp)
    except (ArgumentError, ArgumentTypeError) as e:
        logger.error(f"An error occurred parsing arguments for {sys.argv}")
        raise e
