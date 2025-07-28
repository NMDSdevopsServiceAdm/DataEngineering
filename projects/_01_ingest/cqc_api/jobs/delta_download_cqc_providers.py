import json
import logging
from datetime import date, datetime

from projects._01_ingest.cqc_api.utils import cqc_api as cqc
from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from utils import aws_secrets_manager_utilities as ars
from utils import utils
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ISO_8601_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def main(
    destination: str,
    start_timestamp: str,
    end_timestamp: str,
    cqc_api_primary_key_value: str = "",
):
    start_dt = datetime.fromisoformat(start_timestamp.replace("Z", ""))
    end_dt = datetime.fromisoformat(end_timestamp.replace("Z", ""))

    if start_dt > end_dt:
        raise ValueError("start_timestamp is after end_timestamp")

    spark = utils.get_spark()
    df = None
    if not cqc_api_primary_key_value:
        cqc_api_primary_key_value = json.loads(
            ars.get_secret(secret_name="cqc_api_primary_key", region_name="eu-west-2")
        )["Ocp-Apim-Subscription-Key"]

    logger.info("Collecting providers with changes from API")
    generator = cqc.get_updated_objects(
        object_type="providers",
        organisation_type="provider",
        cqc_api_primary_key=cqc_api_primary_key_value,
        start_timestamp=f"{start_dt.isoformat(timespec='seconds')}Z",
        end_timestamp=f"{end_dt.isoformat(timespec='seconds')}Z",
    )

    df = spark.createDataFrame(generator, PROVIDER_SCHEMA)

    df = df.dropDuplicates([ColNames.provider_id])
    utils.write_to_parquet(df, destination, "append")

    logger.info(f"Finished! Files can be found in {destination}")


if __name__ == "__main__":
    destination_prefix, start_timestamp, end_timestamp, *_ = utils.collect_arguments(
        (
            "--destination_prefix",
            "Source s3 directory for parquet CQC providers dataset",
            False,
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
