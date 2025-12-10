"""Retrieves Provider data from the CQC API."""

import json
import os
import sys
from datetime import date
from datetime import datetime as dt
from typing import Generator

import polars as pl

from polars_utils import utils
from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from schemas.cqc_provider_schema_polars import POLARS_PROVIDER_SCHEMA
from utils.aws_secrets_manager_utilities import get_secret
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)

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
    try:
        destination = destination if destination[-1] == "/" else f"{destination}/"

        start_dt = dt.fromisoformat(start_timestamp.replace("Z", ""))
        end_dt = dt.fromisoformat(end_timestamp.replace("Z", ""))

        if start_dt > end_dt:
            raise InvalidTimestampArgumentError(
                "Start timestamp is after end timestamp"
            )

        print(f'Getting SecretID "{SECRET_ID}"')
        secret = get_secret(secret_name=SECRET_ID, region_name=AWS_REGION)
        cqc_api_primary_key_value: str = json.loads(secret)["Ocp-Apim-Subscription-Key"]

        print("Collecting providers with changes from API")
        api_generator: Generator[dict, None, None] = cqc.get_updated_objects(
            object_type=CQC_OBJECT_TYPE,
            organisation_type=CQC_ORG_TYPE,
            cqc_api_primary_key=cqc_api_primary_key_value,
            start_timestamp=f"{start_dt.isoformat(timespec='seconds')}Z",
            end_timestamp=f"{end_dt.isoformat(timespec='seconds')}Z",
        )

        # Debugging
        for i, record in enumerate(api_generator):
            for col, dtype in POLARS_PROVIDER_SCHEMA.items():
                if isinstance(dtype, pl.Struct):
                    expected_fields = {f.name for f in dtype.fields}
                    actual_fields = (
                        set(record.get(col, {}).keys())
                        if isinstance(record.get(col), dict)
                        else set()
                    )
                    extra_fields = actual_fields - expected_fields
                    if extra_fields:
                        print(
                            f"Row {i} column '{col}' has extra fields: {extra_fields}"
                        )

        generator = cqc.normalised_generator(api_generator, POLARS_PROVIDER_SCHEMA)

        print("Creating dataframe and writing to Parquet")
        df: pl.DataFrame = pl.DataFrame(generator)
        df_schema = df.collect_schema()
        df = df.with_columns(
            [
                pl.col(k).cast(v)
                for k, v in POLARS_PROVIDER_SCHEMA.items()
                if k in df_schema.keys()
            ]
        )
        df_unique: pl.DataFrame = df.unique(subset=[ColNames.provider_id])

        utils.write_to_parquet(df_unique, destination)
        return None

    except InvalidTimestampArgumentError:
        print(f"ERROR: Start timestamp is after end timestamp: Args: {sys.argv}")
        raise
    except FileNotFoundError:
        print(
            f"ERROR: {sys.argv[0]} was unable to write to destination. Args: {sys.argv}"
        )
        raise
    except Exception as e:
        print(
            f"ERROR: An unspecified error occurred while calling the CQC API. Args: {sys.argv}"
        )
        print(f"ERROR: {e.with_traceback(e.__traceback__)}")
        raise


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--destination_prefix",
            "Source s3 directory for parquet CQC providers dataset",
        ),
        ("--start_timestamp", "Start timestamp for provider changes"),
        ("--end_timestamp", "End timestamp for provider changes"),
    )
    print(f"Running job from {args.start_timestamp} to {args.end_timestamp}")

    todays_date = date.today()
    destination = utils.generate_s3_dir(
        destination_prefix=args.destination_prefix,
        domain="CQC",
        dataset="delta_providers_api",
        date=todays_date,
        version="3.0.0",
    )
    main(destination, args.start_timestamp, args.end_timestamp)
