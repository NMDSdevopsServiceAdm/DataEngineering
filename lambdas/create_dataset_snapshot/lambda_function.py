import json
import logging

from re import match
from datetime import datetime

from s3fs import S3FileSystem

from utils import build_snapshot_table_from_delta

from ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Received event: \n" + json.dumps(event, indent=2))

    input_parse = match(
        "s3://(?P<bucket>[\w\-=.]+)/(?P<read_folder>[\w/-=.]+)", event["input_uri"]
    )

    date_int = int(
        datetime.strptime(event["snapshot_date"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
            "%Y%m%d"
        )
    )
    logger.info(
        f"bucket={input_parse.group('bucket')}, read_folder={input_parse.group('read_folder')}"
    )

    snapshot_df = build_snapshot_table_from_delta(
        bucket=input_parse.group("bucket"),
        read_folder=input_parse.group("read_folder"),
        timepoint=date_int,
    )

    output_uri = event["output_uri"] + f"import_date={date_int}/file.parquet"

    fs = S3FileSystem()
    with fs.open(output_uri, mode="wb") as destination:
        snapshot_df.drop([Keys.year, Keys.month, Keys.day]).write_parquet(
            destination, compression="snappy"
        )
    logger.info(
        f"Finished processing snapshot {event['snapshot_date']}. The files can be found at {event['output_uri']}"
    )
